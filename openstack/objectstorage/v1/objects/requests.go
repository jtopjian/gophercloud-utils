package objects

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	"log"

	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/openstack/objectstorage/v1/containers"
	"github.com/gophercloud/gophercloud/openstack/objectstorage/v1/objects"
)

const (
	emptyETag = "d41d8cd98f00b204e9800998ecf8427e"
)

var (
	knownDirMarkers = []string{
		"application/directory",
		"text/directory",
	}
)

// UploadOpts represents options used for uploading an object.
type UploadOpts struct {
	// Changed will prevent an upload if the mtime and size of the source
	// and destination objects are the same.
	Changed bool

	// Checksum will enforce a comparison of the md5sum/etag between the
	// local and remote object to ensure integrity.
	Checksum bool

	// Content is an io.Reader which can be used to upload a object via an
	// open file descriptor or any other type of stream.
	Content io.Reader

	// DirMarker will create a directory marker.
	DirMarker bool

	// LeaveSegments will cause old segments of an object to be left in a
	// container.
	LeaveSegments bool

	// Metadata is optional metadata to place on the object.
	Metadata map[string]string

	// Path is a local filesystem path of an object to be uploaded.
	Path string

	// Segment container is a custom container name to store object segments.
	// If one is not specified, then "containerName_segments" will be used.
	SegmentContainer string

	// SegmentSize is the size of each segment. An object will be split into
	// pieces (segments) of this size.
	SegmentSize int64

	// SkipIdentical is a more thorough check than "Changed". It will compare
	// the md5sum/etag of the object as a comparison.
	SkipIdentical bool

	// StoragePolicy represents a storage policy of where the object should be
	// uploaded.
	StoragePolicy string

	// UseSLO will have the object uploaded using Static Large Object support.
	UseSLO bool
}

// uploadHelperOpts is an internal structure. It embeds UploadOpts and adds
// a few extra fields which are used throughout the internal functions.
type uploadHelperOpts struct {
	UploadOpts
	sourceFileInfo     os.FileInfo
	origObjectExists   bool
	origObjectHeaders  *objects.GetHeader
	origObjectMetadata map[string]string
}

// uploadSegmentOpts is an internal structure used for handling the upload
// of an object's segment.
type uploadSegmentOpts struct {
	Checksum         bool
	ContainerName    string
	Content          io.Reader
	Path             string
	ObjectName       string
	SegmentContainer string
	SegmentName      string
	SegmentSize      int64
	SegmentStart     int64
	SegmentIndex     int
}

// uploadSegmentResult is an internal structure that represents the result
// result of a segment upload.
type uploadSegmentResult struct {
	Complete bool
	ETag     string
	Index    int
	Location string
	Size     int64
	Success  bool
}

// uploadSLOManifestOpts is an internal structure that represents
// options used for creating an SLO manifest.
type uploadSLOManifestOpts struct {
	Results       []uploadSegmentResult
	ContainerName string
	ObjectName    string
	Metadata      map[string]string
}

// sloManifest represents an SLO manifest.
type sloManifest struct {
	Path      string `json:"path"`
	ETag      string `json:"etag"`
	SizeBytes int64  `json:"size_bytes"`
}

// https://github.com/openstack/python-swiftclient/blob/e65070964c7b1e04119c87e5f344d39358780d18/swiftclient/service.py#L1371
func Upload(client *gophercloud.ServiceClient, containerName, objectName string, uploadOpts UploadOpts) error {
	if uploadOpts.Path != "" && uploadOpts.Content != nil {
		return fmt.Errorf("Only one of Path and Content can be used")
	}

	containerName, pseudoFolder := ContainerPartition(containerName)
	if pseudoFolder != "" {
		objectName = pseudoFolder + "/" + objectName
	}

	if strings.HasPrefix(objectName, `./`) || strings.HasPrefix(objectName, `.\`) {
		objectName = string(objectName[:2])
	}

	if strings.HasPrefix(objectName, `/`) {
		objectName = string(objectName[:1])
	}

	if len(uploadOpts.Metadata) == 0 {
		uploadOpts.Metadata = make(map[string]string)
	}

	opts := &uploadHelperOpts{
		UploadOpts: uploadOpts,
	}

	// Try to create the container, but ignore any errors.
	// TODO: add X-Storage-Policy to Gophercloud.
	// If a storage policy was specified, create the container with that policy.
	containers.Create(client, containerName, nil)

	// Check and see if the object being requested already exists.
	opts.origObjectExists = true
	objectResult := objects.Get(client, containerName, objectName, nil)
	if objectResult.Err != nil {
		if _, ok := objectResult.Err.(gophercloud.ErrDefault404); ok {
			opts.origObjectExists = false
		} else {
			return fmt.Errorf("error retrieving original object %s/%s: %s", containerName, objectName, objectResult.Err)
		}
	}

	// If it already exists, stash its headers and metadata for later comparisons.
	if opts.origObjectExists {
		headers, err := objectResult.Extract()
		if err != nil {
			return fmt.Errorf("error extracting headers of original object %s/%s: %s", containerName, objectName, err)
		}
		opts.origObjectHeaders = headers

		metadata, err := objectResult.ExtractMetadata()
		if err != nil {
			return fmt.Errorf("error extracting metadata of original object %s/%s: %s", containerName, objectName, err)
		}
		opts.origObjectMetadata = metadata
	}

	// Figure out the mtime.
	// If a path was specified, then use the file's mtime.
	// Otherwise, use the current time.
	if opts.Path != "" {
		fileInfo, err := os.Stat(opts.Path)
		if err != nil {
			return fmt.Errorf("error retrieving file stats of %s: %s", opts.Path, err)
		}

		// store the file's fileInfo for later reference.
		opts.sourceFileInfo = fileInfo

		// Format the file's mtime in the same format used by python-swiftclient.
		v := fileInfo.ModTime().UnixNano()
		mtime := fmt.Sprintf("%.6f", float64(v)/1000000000)
		opts.Metadata["Mtime"] = mtime
	} else {
		v := time.Now().UnixNano()
		mtime := fmt.Sprintf("%.6f", float64(v)/1000000000)
		opts.Metadata["Mtime"] = mtime
	}

	// If a segment size was specified, then the object will most likely
	// be broken up into segments.
	if opts.SegmentSize != 0 {
		// First determine what the segment container will be called.
		if opts.SegmentContainer == "" {
			opts.SegmentContainer = containerName + "_segments"
		}

		// Then create the segment container.
		// TODO: add X-Storage-Policy to Gophercloud.
		// Create the segment container in either the specified policy or the same
		// policy as the above container.
		res := containers.Create(client, opts.SegmentContainer, nil)
		if res.Err != nil {
			return res.Err
		}
	}

	// If an io.Reader (streaming) was specified...
	if opts.Content != nil {
		log.Printf("[DEBUG] If an io.Reader (streaming) was specified...")
		err := uploadObject(client, containerName, objectName, opts)
		if err != nil {
			return fmt.Errorf("error uploading object %s/%s: %s", containerName, objectName, err)
		}

		return nil
	}

	// If a local path was specified...
	if opts.Path != "" {
		log.Printf("[DEBUG] If a local path was specified...")
		if opts.sourceFileInfo.IsDir() {
			// If the source path is a directory, then create a Directory Marker,
			// even if DirMarker wasn't specified.
			err := createDirMarker(client, containerName, objectName, opts)
			if err != nil {
				return fmt.Errorf("error creating directory marker %s/%s: %s", containerName, objectName, err)
			}

			return nil
		}

		err := uploadObject(client, containerName, objectName, opts)
		if err != nil {
			return fmt.Errorf("error uploading object %s/%s: %s", containerName, objectName, err)
		}

		return nil
	}

	if opts.DirMarker {
		err := createDirMarker(client, containerName, objectName, opts)
		if err != nil {
			return fmt.Errorf("error creating directory marker %s/%s: %s", containerName, objectName, err)
		}

		return nil
	}

	// Finally, create an empty object.
	opts.Content = strings.NewReader("")
	err := uploadObject(client, containerName, objectName, opts)
	if err != nil {
		return fmt.Errorf("error creating empty object %s/%s: %s", containerName, objectName, err)
	}

	return nil
}

// createDirMarker will create a pseudo-directory in Swift.
//
// https://github.com/openstack/python-swiftclient/blob/e65070964c7b1e04119c87e5f344d39358780d18/swiftclient/service.py#L1656
func createDirMarker(client *gophercloud.ServiceClient, containerName, objectName string, opts *uploadHelperOpts) error {
	if opts.origObjectExists {
		if opts.Changed {
			contentLength := opts.origObjectHeaders.ContentLength
			eTag := opts.origObjectHeaders.ETag

			v := strings.SplitN(opts.origObjectHeaders.ContentType, ";", 2)
			contentType := v[0]

			var mtMatch bool
			if origMTime, ok := opts.origObjectMetadata["Mtime"]; ok {
				if newMTime, ok := opts.Metadata["Mtime"]; ok {
					if origMTime == newMTime {
						mtMatch = true
					}
				}
			}

			var ctMatch bool
			for _, kdm := range knownDirMarkers {
				if contentType == kdm {
					ctMatch = true
				}
			}

			if ctMatch && mtMatch && contentLength == 0 && eTag == emptyETag {
				return nil
			}
		}
	}

	createOpts := objects.CreateOpts{
		Content:       strings.NewReader(""),
		ContentLength: 0,
		ContentType:   "application/directory",
		Metadata:      opts.Metadata,
	}

	res := objects.Create(client, containerName, objectName, createOpts)
	if res.Err != nil {
		return res.Err
	}

	return nil
}

// uploadObject handles uploading an object to Swift.
// This includes support for SLO, DLO, and standard uploads
// from both streaming sources and local file paths.
//
// https://github.com/openstack/python-swiftclient/blob/e65070964c7b1e04119c87e5f344d39358780d18/swiftclient/service.py#L2006
func uploadObject(client *gophercloud.ServiceClient, containerName, objectName string, opts *uploadHelperOpts) error {
	// manifestData contains information about existing objects.
	var manifestData []Manifest

	// oldObjectManifest is the existing object's manifest.
	var oldObjectManifest string

	// oldSLOManifestPaths is a list of the old object segment's manifest paths.
	var oldSLOManifestPaths []string

	// newSLOManifestPaths is a list of the new object segment's manifest paths.
	var newSLOManifestPaths []string

	if opts.origObjectExists {
		origHeaders := opts.origObjectHeaders
		origMetadata := opts.origObjectMetadata
		isSLO := origHeaders.StaticLargeObject

		if opts.Changed || opts.SkipIdentical || !opts.LeaveSegments {
			var err error

			// If the below conditionals are met, get the manifest data of
			// the existing object.
			if opts.SkipIdentical || (isSLO && !opts.LeaveSegments) {
				mo := GetManifestOpts{
					ContainerName: containerName,
					ObjectName:    objectName,
					Headers:       origHeaders,
				}

				manifestData, err = GetManifest(client, mo)
				if err != nil {
					return fmt.Errorf("unable to get manifest for %s/%s: %s", containerName, objectName, err)
				}

				log.Printf("[DEBUG] manifest data: %#v", manifestData)
			}

			// If SkipIdentical is enabled, compare the md5sum/etag of each
			// piece of the manifest to determine if the objects are the same.
			if opts.SkipIdentical {
				ok, err := IsIdentical(manifestData, opts.Path)
				if err != nil {
					return fmt.Errorf("error comparing object %s/%s and path %s: %s", containerName, objectName, opts.Path, err)
				}

				if ok {
					log.Printf("[DEBUG] %s/%s and %s are identical", containerName, objectName, opts.Path)
					return nil
				}
			}
		}

		// If the source object is a local file and Changed is enabled,
		// compare the mtime and content length to determine if the objects
		// are the same.
		if opts.Path != "" && opts.Changed {
			var mtMatch bool
			if v, ok := origMetadata["Mtime"]; ok {
				if v == opts.Metadata["Mtime"] {
					mtMatch = true
				}
			}

			var fSizeMatch bool
			if origHeaders.ContentLength == opts.sourceFileInfo.Size() {
				fSizeMatch = true
			}

			if mtMatch && fSizeMatch {
				return nil
			}
		}

		// If LeaveSegments is set to false (default), keep
		// track of the paths of the original object's segments.
		if !opts.LeaveSegments {
			oldObjectManifest = origHeaders.ObjectManifest
			log.Printf("[DEBUG] %s", oldObjectManifest)

			if isSLO {
				for _, data := range manifestData {
					segPath := strings.TrimSuffix(data.Name, "/")
					oldSLOManifestPaths = append(oldSLOManifestPaths, segPath)
				}
			}
		}
	}

	// Segment upload
	if opts.Path != "" && opts.SegmentSize > 0 && (opts.sourceFileInfo.Size() > opts.SegmentSize) {
		var uploadResults []uploadSegmentResult

		var segStart int64
		var segIndex int
		fSize := opts.sourceFileInfo.Size()
		segSize := opts.SegmentSize

		log.Printf("[DEBUG] here 3")
		for segStart < fSize {
			var segName string
			log.Printf("[DEBUG] here 4")

			if segStart+segSize > fSize {
				segSize = fSize - segStart
			}

			log.Printf("[DEBUG] segSize: %v", segSize)
			if opts.UseSLO {
				segName = fmt.Sprintf("%s/slo/%s/%d/%d/%08d",
					objectName, opts.Metadata["Mtime"], fSize, opts.SegmentSize, segIndex)
			} else {
				segName = fmt.Sprintf("%s/%s/%d/%d/%08d",
					objectName, opts.Metadata["Mtime"], fSize, opts.SegmentSize, segIndex)
			}

			uploadOpts := &uploadSegmentOpts{
				Checksum:         opts.Checksum,
				Path:             opts.Path,
				ObjectName:       objectName,
				SegmentContainer: opts.SegmentContainer,
				SegmentIndex:     segIndex,
				SegmentName:      segName,
				SegmentSize:      segSize,
				SegmentStart:     segStart,
			}

			log.Printf("[DEBUG] uploadOpts: %#v", uploadOpts)

			result, err := uploadSegment(client, uploadOpts)
			if err != nil {
				return err
			}

			uploadResults = append(uploadResults, *result)

			segIndex += 1
			segStart += segSize
		}

		if opts.UseSLO {
			uploadOpts := &uploadSLOManifestOpts{
				Results:       uploadResults,
				ContainerName: containerName,
				ObjectName:    objectName,
				Metadata:      opts.Metadata,
			}

			err := uploadSLOManifest(client, uploadOpts)
			if err != nil {
				return err
			}

			for _, result := range uploadResults {
				newSLOManifestPaths = append(newSLOManifestPaths, result.Location)
			}

		} else {
			newObjectManifest := fmt.Sprintf("%s/%s/%s/%d/%d/",
				url.QueryEscape(opts.SegmentContainer), url.QueryEscape(objectName),
				opts.Metadata["Mtime"], fSize, opts.SegmentSize)

			if oldObjectManifest != "" {
				if strings.TrimSuffix(oldObjectManifest, "/") == strings.TrimSuffix(newObjectManifest, "/") {
					oldObjectManifest = ""
				}
			}

			createOpts := objects.CreateOpts{
				Content:        strings.NewReader(""),
				ContentLength:  0,
				Metadata:       opts.Metadata,
				ObjectManifest: newObjectManifest,
			}

			res := objects.Create(client, containerName, objectName, createOpts)
			if res.Err != nil {
				return res.Err
			}
		}
	} else if opts.UseSLO && opts.SegmentSize > 0 && opts.Path == "" {
		// Streaming segment upload
		var segIndex int
		var uploadResults []uploadSegmentResult

		for {
			segName := fmt.Sprintf("%s/slo/%d/%d/%08d",
				objectName, opts.Metadata["Mtime"], opts.SegmentSize, segIndex)

			uploadOpts := &uploadSegmentOpts{
				Checksum:      opts.Checksum,
				Content:       opts.Content,
				ContainerName: containerName,
				ObjectName:    objectName,
				SegmentIndex:  segIndex,
				SegmentName:   segName,
				SegmentSize:   opts.SegmentSize,
			}

			uploadResult, err := uploadStreamingSegment(client, uploadOpts)
			if err != nil {
				return err
			}

			uploadResults = append(uploadResults, *uploadResult)

			if !uploadResult.Success {
				return fmt.Errorf("Problem uploading segment %d of %s/%s", segIndex, containerName, objectName)
			}

			if uploadResult.Complete {
				break
			}

			segIndex += 1
		}

		if len(uploadResults) > 0 {
			if uploadResults[0].Location != fmt.Sprintf("/%s/%s", containerName, objectName) {
				uploadOpts := &uploadSLOManifestOpts{
					Results:       uploadResults,
					ContainerName: containerName,
					ObjectName:    objectName,
					Metadata:      opts.Metadata,
				}

				err := uploadSLOManifest(client, uploadOpts)
				if err != nil {
					return err
				}

				for _, result := range uploadResults {
					newSLOManifestPaths = append(newSLOManifestPaths, result.Location)
				}
			}
		}
	} else {
		var reader io.Reader
		var contentLength int64

		if opts.Path != "" {
			f, err := os.Open(opts.Path)
			if err != nil {
				return err
			}
			defer f.Close()

			reader = f
			contentLength = opts.sourceFileInfo.Size()
		} else {
			reader = opts.Content
		}

		createOpts := objects.CreateOpts{
			Content:       reader,
			ContentLength: contentLength,
			Metadata:      opts.Metadata,
		}

		_, err := objects.Create(client, containerName, objectName, createOpts).Extract()
		if err != nil {
			return err
		}

		/*
			TODO
			if opts.Checksum {
		*/
	}

	if oldObjectManifest != "" || len(oldSLOManifestPaths) > 0 {
		delObjectMap := make(map[string][]string)
		if oldObjectManifest != "" {
			var oldObjects []string

			parts := strings.SplitN(oldObjectManifest, "/", 2)
			sContainer := parts[0]
			sPrefix := parts[1]

			sPrefix = strings.TrimRight(sPrefix, "/") + "/"

			listOpts := objects.ListOpts{
				Prefix: sPrefix,
			}
			allPages, err := objects.List(client, sContainer, listOpts).AllPages()
			if err != nil {
				return err
			}

			allObjects, err := objects.ExtractNames(allPages)
			if err != nil {
				return err
			}

			for _, o := range allObjects {
				oldObjects = append(oldObjects, o)
			}

			delObjectMap[sContainer] = oldObjects
		}

		if len(oldSLOManifestPaths) > 0 {
			for _, segToDelete := range oldSLOManifestPaths {
				var oldObjects []string

				var exists bool
				for _, newSeg := range newSLOManifestPaths {
					if segToDelete == newSeg {
						exists = true
					}
				}

				if !exists {
					parts := strings.SplitN(segToDelete, "/", 2)
					sContainer := parts[0]
					sObject := parts[1]

					if _, ok := delObjectMap[sContainer]; ok {
						oldObjects = delObjectMap[sContainer]
					}

					oldObjects = append(oldObjects, sObject)
					delObjectMap[sContainer] = oldObjects
				}
			}
		}

		for sContainer, oldObjects := range delObjectMap {
			for _, oldObject := range oldObjects {
				res := objects.Delete(client, sContainer, oldObject, nil)
				if res.Err != nil {
					return res.Err
				}
			}
		}
	}

	return nil
}

// https://github.com/openstack/python-swiftclient/blob/e65070964c7b1e04119c87e5f344d39358780d18/swiftclient/service.py#L1966
func uploadSLOManifest(client *gophercloud.ServiceClient, opts *uploadSLOManifestOpts) error {
	var manifest []sloManifest
	for _, result := range opts.Results {
		m := sloManifest{
			Path:      result.Location,
			ETag:      result.ETag,
			SizeBytes: result.Size,
		}

		log.Printf("[DEBUG] segment: %#v", m)
		manifest = append(manifest, m)
	}

	b, err := json.Marshal(manifest)
	if err != nil {
		return err
	}

	createOpts := objects.CreateOpts{
		Content:           strings.NewReader(string(b)),
		ContentType:       "application/json",
		Metadata:          opts.Metadata,
		MultipartManifest: "put",
		NoETag:            true,
	}

	res := objects.Create(client, opts.ContainerName, opts.ObjectName, createOpts)
	if res.Err != nil {
		return res.Err
	}

	return nil
}

// https://github.com/openstack/python-swiftclient/blob/e65070964c7b1e04119c87e5f344d39358780d18/swiftclient/service.py#L1719
func uploadSegment(client *gophercloud.ServiceClient, opts *uploadSegmentOpts) (*uploadSegmentResult, error) {
	f, err := os.Open(opts.Path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	_, err = f.Seek(opts.SegmentStart, 0)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, opts.SegmentSize)
	n, err := f.Read(buf)
	if err != nil && err != io.EOF {
		return nil, err
	}

	createOpts := objects.CreateOpts{
		ContentLength: int64(n),
		ContentType:   "application/swiftclient-segment",
		Content:       bytes.NewReader(buf),
	}

	createHeader, err := objects.Create(client, opts.SegmentContainer, opts.SegmentName, createOpts).Extract()
	if err != nil {
		return nil, err
	}

	if opts.Checksum {
		hash := md5.New()
		hash.Write(buf)
		etag := fmt.Sprintf("%x", hash.Sum(nil))

		if createHeader.ETag != etag {
			err := fmt.Errorf("Segment %d: upload verification failed: md5 mismatch, local %s != remote %s", opts.SegmentIndex, etag, createHeader.ETag)
			return nil, err
		}

	}

	result := &uploadSegmentResult{
		ETag:     createHeader.ETag,
		Index:    opts.SegmentIndex,
		Location: fmt.Sprintf("/%s/%s", opts.SegmentContainer, opts.SegmentName),
		Size:     opts.SegmentSize,
	}

	return result, nil
}

// https://github.com/openstack/python-swiftclient/blob/e65070964c7b1e04119c87e5f344d39358780d18/swiftclient/service.py#L1846
func uploadStreamingSegment(client *gophercloud.ServiceClient, opts *uploadSegmentOpts) (*uploadSegmentResult, error) {
	var result uploadSegmentResult

	hash := md5.New()
	v := make([]byte, opts.SegmentSize)
	buf := bytes.NewBuffer(v)
	n, err := io.CopyN(io.MultiWriter(hash, buf), opts.Content, opts.SegmentSize)
	if err != nil && err != io.EOF {
		return nil, err
	}

	localChecksum := fmt.Sprintf("%x", hash.Sum(nil))

	if n == 0 {
		result.Complete = true
		result.Success = true
		result.Size = 0

		return &result, nil
	}

	createOpts := objects.CreateOpts{
		Content:       bytes.NewReader(v),
		ContentLength: n,
		// TODO
		//Metadata: opts.Metadata,
		ETag: localChecksum,
	}

	if opts.SegmentIndex == 0 {
		res := objects.Create(client, opts.ContainerName, opts.ObjectName, createOpts)
		if res.Err != nil {
			return nil, res.Err
		}

		result.Location = fmt.Sprintf("/%s/%s", opts.ContainerName, opts.ObjectName)
	} else {
		res := objects.Create(client, opts.SegmentContainer, opts.SegmentName, createOpts)
		if res.Err != nil {
			return nil, res.Err
		}

		result.Location = fmt.Sprintf("/%s/%s", opts.SegmentContainer, opts.SegmentName)
	}

	result.Success = true
	result.Complete = n < opts.SegmentSize
	result.Size = n
	result.Index = opts.SegmentIndex
	result.ETag = localChecksum

	return &result, nil
}
