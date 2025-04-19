package telemetry

import (
	"context"

	"github.com/pragyandas/hydra/common"
	"github.com/uptrace/opentelemetry-go-extra/otelzap"
	"go.uber.org/zap"
)

var logger *otelzap.Logger

func GetLogger(ctx context.Context, component string) otelzap.LoggerWithCtx {
	if logger == nil {
		zapLogger, err := zap.NewProduction()
		if err != nil {
			panic(err)
		}
		logger = otelzap.New(zapLogger, otelzap.WithMinLevel(zap.InfoLevel))
	}

	return logger.Ctx(ctx).WithOptions(
		zap.Fields(
			zap.String("component", component),
			zap.String("system_id", common.GetSystemID(ctx)),
			zap.String("region", common.GetRegion(ctx)),
		),
	)
}
