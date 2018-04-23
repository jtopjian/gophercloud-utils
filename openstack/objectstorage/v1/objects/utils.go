package objects

import (
	"strings"
)

func ContainerPartition(containerName string) (string, string) {
	var pseudoFolder string

	parts := strings.SplitN(containerName, "/", 2)
	if len(parts) == 2 {
		containerName = parts[0]
		pseudoFolder = strings.TrimSuffix(parts[1], "/")
	}

	return containerName, pseudoFolder
}
