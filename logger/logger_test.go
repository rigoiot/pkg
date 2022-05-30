package logger_test

import (
	"fmt"
	"testing"

	"github.com/rigoiot/pkg/logger"
	"github.com/sirupsen/logrus"
)

func TestLoggerInitilization(t *testing.T) {

	logger.Init("./log", 7, 86400, "debug")

	logger.Infof("testing log")
}

func TestCustomHook(t *testing.T) {
	logger.Init("./log", 7, 86400, "error")

	logger.SetCustomHook(func(entry *logrus.Entry) error {
		fmt.Printf("level is:%s, msg:%s\n", entry.Level, entry.Message)
		return nil
	})

	logger.Debugf("testing debug level")

	logger.Infof("testing info level")

	logger.Errorf("testing error level")

}
