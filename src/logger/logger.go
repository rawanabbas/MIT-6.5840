package Logger

import (
	"go.uber.org/zap"
)

func NewLogger(filename string) *zap.SugaredLogger {
	cfg := zap.NewDevelopmentConfig()
	cfg.OutputPaths = append(cfg.OutputPaths, filename)
	logger, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	return logger.Sugar()
}
