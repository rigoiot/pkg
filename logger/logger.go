package logger

import (
	_log "log"
	"os"

	"github.com/gggwvg/logrotate"
	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
)

// log for logging
var log *logrus.Logger

// Init a logger
func Init(logPath string, maxArchives int, rotateSize, level string) *logrus.Logger {
	if log != nil {
		return log
	}

	os.MkdirAll(logPath, 0777)

	infoWriter, err := logrotate.NewLogger(logrotate.File(logPath+"/info.log"), logrotate.RotatePeriod(logrotate.PeriodDaily), logrotate.MaxArchives(maxArchives), logrotate.RotateSize(rotateSize))

	if err != nil {
		_log.Println(err.Error())
	}

	errorWriter, err := logrotate.NewLogger(logrotate.File(logPath+"/error.log"), logrotate.RotatePeriod(logrotate.PeriodDaily), logrotate.MaxArchives(maxArchives), logrotate.RotateSize(rotateSize))
	if err != nil {
		_log.Println(err.Error())
	}

	log = logrus.New()
	switch level {
	case "debug":
		log.Level = logrus.DebugLevel
	case "info":
		log.Level = logrus.InfoLevel
	case "error":
		log.Level = logrus.ErrorLevel
	default:
		log.Level = logrus.ErrorLevel
	}

	log.Formatter = new(logrus.JSONFormatter)
	log.Hooks.Add(lfshook.NewHook(
		lfshook.WriterMap{
			logrus.DebugLevel: infoWriter,
			logrus.InfoLevel:  infoWriter,
			logrus.ErrorLevel: errorWriter,
		},
		&logrus.JSONFormatter{}))

	return log
}

// Logger return the logrus logger
func Logger() *logrus.Logger {
	return log
}

// Debugf log debug info
func Debugf(format string, args ...interface{}) {
	if log != nil {
		log.Debugf(format, args...)
	} else {
		_log.Printf(format, args...)
	}
}

// Infof log normal info
func Infof(format string, args ...interface{}) {
	if log != nil {
		log.Infof(format, args...)
	} else {
		_log.Printf(format, args...)
	}
}

// Printf log normal info
func Printf(format string, args ...interface{}) {
	if log != nil {
		log.Printf(format, args...)
	} else {
		_log.Printf(format, args...)
	}
}

// Warnf log warnning info
func Warnf(format string, args ...interface{}) {
	if log != nil {
		log.Warnf(format, args...)
	} else {
		_log.Printf(format, args...)
	}
}

// Errorf log error info
func Errorf(format string, args ...interface{}) {
	if log != nil {
		log.Errorf(format, args...)
	} else {
		_log.Printf(format, args...)
	}
}

// Fatalf log fatal info
func Fatalf(format string, args ...interface{}) {
	if log != nil {
		log.Fatalf(format, args...)
	} else {
		_log.Fatalf(format, args...)
	}
}

// Panicf log panic info
func Panicf(format string, args ...interface{}) {
	if log != nil {
		log.Panicf(format, args...)
	} else {
		_log.Panicf(format, args...)
	}
}

// Debug log debug info
func Debug(args ...interface{}) {
	if log != nil {
		log.Debug(args...)
	} else {
		_log.Print(args...)
	}
}

// Info log normal info
func Info(args ...interface{}) {
	if log != nil {
		log.Info(args...)
	} else {
		_log.Print(args...)
	}
}

// Print log normal info
func Print(args ...interface{}) {
	if log != nil {
		log.Info(args...)
	} else {
		_log.Print(args...)
	}
}

// Warn log warn info
func Warn(args ...interface{}) {
	if log != nil {
		log.Warn(args...)
	} else {
		_log.Print(args...)
	}
}

// Error log error info
func Error(args ...interface{}) {
	if log != nil {
		log.Error(args...)
	} else {
		_log.Print(args...)
	}
}

// Fatal log fatal info
func Fatal(args ...interface{}) {
	if log != nil {
		log.Fatal(args...)
	} else {
		_log.Fatal(args...)
	}
}

// Println ...
func Println(args ...interface{}) {
	if log != nil {
		log.Println(args...)
	} else {
		_log.Println(args...)
	}
}
