package common

import "os"

const (
	EnvSystemID = "SYSTEM_ID"
	EnvRegion   = "REGION"
)

func GetSystemID() string {
	if envSystemID := os.Getenv(EnvSystemID); envSystemID != "" {
		return envSystemID
	}
	return "actorsystem"
}

func GetRegion() string {
	if envRegion := os.Getenv(EnvRegion); envRegion != "" {
		return envRegion
	}
	return "local"
}
