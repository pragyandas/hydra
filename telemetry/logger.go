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
		l := otelzap.New(zapLogger,
			otelzap.WithExtraFields(
				zap.String("system_id", common.GetSystemID()),
				zap.String("region", common.GetRegion()),
			),
			otelzap.WithMinLevel(zap.InfoLevel),
		)

		logger = l
	}

	return logger.Ctx(ctx).WithOptions(zap.Fields(zap.String("component", component)))
}
