package common

import "context"

type contextKey string

const (
	SystemIDKey = contextKey("system_id")
	RegionKey   = contextKey("region")
)

func GetSystemID(ctx context.Context) string {
	return ctx.Value(SystemIDKey).(string)
}

func GetRegion(ctx context.Context) string {
	return ctx.Value(RegionKey).(string)
}
