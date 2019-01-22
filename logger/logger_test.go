package logger_test

import (
	"testing"

	"github.com/rigoiot/pkg/logger"
)

func TestLoggerInitilization(t *testing.T) {

	logger.Init("./log", 7, 86400, true)

	logger.Infof("testing log")
}
