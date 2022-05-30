package logger

import (
	"github.com/sirupsen/logrus"
)

var custHook *customHook

type customHook struct {
	levels       []logrus.Level
	customHandel func(*logrus.Entry) error
}

func (s *customHook) Levels() []logrus.Level {
	return s.levels
}

func (s *customHook) Fire(entry *logrus.Entry) error {
	if s.customHandel != nil {
		return s.customHandel(entry)
	}
	return nil
}

func SetCustomHook(fc func(*logrus.Entry) error) {
	if custHook == nil {
		custHook = &customHook{
			levels:       logrus.AllLevels,
			customHandel: fc,
		}
		log.Hooks.Add(custHook)
	}
}
