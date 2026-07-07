package metrics

import (
	"driver-service/internal/common/decorator"

	"github.com/sirupsen/logrus"
)

// LoggingMetricsClient logs all metrics to logrus
type LoggingMetricsClient struct {
	logger *logrus.Entry
}

func NewLoggingMetricsClient(logger *logrus.Entry) decorator.MetricsClient {
	return &LoggingMetricsClient{logger: logger}
}

func (l *LoggingMetricsClient) Inc(key string, value int) {
	l.logger.WithFields(logrus.Fields{
		"metric_key": key,
		"value":      value,
	}).Info("Metric recorded")
}

// NoopMetricsClient discards all metrics
type NoopMetricsClient struct{}

func NewNoopMetricsClient() decorator.MetricsClient {
	return &NoopMetricsClient{}
}

func (n *NoopMetricsClient) Inc(key string, value int) {}
