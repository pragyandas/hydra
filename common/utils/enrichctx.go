package utils

import (
	"context"

	"github.com/pragyandas/hydra/common"
)

func EnrichContext(ctx context.Context, id, region string) context.Context {
	ctx = context.WithValue(ctx, common.SystemIDKey, id)
	ctx = context.WithValue(ctx, common.RegionKey, region)

	return ctx
}
