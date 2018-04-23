// +build acceptance

package v1

import (
	"testing"

	"github.com/gophercloud/gophercloud/acceptance/clients"
	//"github.com/gophercloud/gophercloud/acceptance/tools"
	//c "github.com/gophercloud/gophercloud/openstack/objectstorage/v1/containers"
	th "github.com/gophercloud/gophercloud/testhelper"

	"github.com/gophercloud/utils/openstack/objectstorage/v1/objects"
)

func TestObjectUpload(t *testing.T) {
	client, err := clients.NewObjectStorageV1Client()
	th.AssertNoErr(t, err)

	//cName := tools.RandomString("test-container-", 8)
	//oName := tools.RandomString("test-object-", 8)

	uploadOpts := objects.UploadOpts{
		Path:        "/home/centos/g.img",
		SegmentSize: 104857600,
		UseSLO:      true,
	}

	err = objects.Upload(client, "test-container-ejV422qX", "foo", uploadOpts)
	th.AssertNoErr(t, err)

	/*
		defer func() {
			c.Delete(client, cName)
		}()
	*/
}
