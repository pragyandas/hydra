package common

import "os"

const (
	EnvSystemID = "SYSTEM_ID"
	EnvRegion   = "REGION"
)

var systemID, region string

func GetSystemID() string {
	if systemID != "" {
		return systemID
	}

	if envSystemID := os.Getenv(EnvSystemID); envSystemID != "" {
		systemID = envSystemID
	} else {
		systemID = "actorsystem"
	}

	return systemID
}

func GetRegion() string {
	if region != "" {
		return region
	}

	if envRegion := os.Getenv(EnvRegion); envRegion != "" {
		region = envRegion
	} else {
		region = "local"
	}

	return region
}
