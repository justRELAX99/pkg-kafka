package logger

import (
	"github.com/sirupsen/logrus"
	"github.com/vm-affekt/logrustash"

	"time"
)

const (
	defaultLogLevel = logrus.DebugLevel
)

var (
	defaultFields = logrus.Fields{"tool": "kafka"}
	logger        *logrus.Entry
)

func SetLogger(l logrus.FieldLogger) {
	if l == nil {
		l = initDefaultLogger()
	}
	logger = l.WithFields(defaultFields)
}

func initDefaultLogger() *logrus.Logger {
	l := logrus.New()
	l.SetLevel(defaultLogLevel)
	l.SetFormatter(
		&logrustash.LogstashFormatter{
			TimestampFormat: time.RFC3339Nano,
		},
	)
	return l
}

func GetLogger() logrus.FieldLogger {
	return logger
}
