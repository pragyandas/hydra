package common

// Singleton instance for quick access to commonly used environment variables
var env Env

type Env struct {
	SystemID string
	Region   string
}

func SetEnv(systemID, region string) {
	env = Env{
		SystemID: systemID,
		Region:   region,
	}
}

func GetSystemID() string {
	return env.SystemID
}

func GetRegion() string {
	return env.Region
}
