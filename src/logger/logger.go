package Logger

import (
	"fmt"
	"os"

	"go.uber.org/zap"
)

var cfg zap.Config

func NewLogger(filename string) *zap.SugaredLogger {
	cfg = zap.NewDevelopmentConfig()
	if _, err := os.Stat(filename); err == nil {
		f, fileErr := os.OpenFile(filename, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
		if fileErr != nil {
			fmt.Println("Truncated", filename)
			f.Close()
		} else {
			fmt.Println("Failed to truncate", filename)
		}
	}
	cfg.OutputPaths = append(cfg.OutputPaths, filename)

	logger, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	return logger.Sugar()
}

func SetDebugOff() {
	cfg.Level.SetLevel(zap.WarnLevel)
}
