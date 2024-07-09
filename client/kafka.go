package client

import (
	"github.com/enkodio/pkg-kafka/internal/kafka/logic"
	"github.com/enkodio/pkg-kafka/internal/pkg/logger"
	"github.com/enkodio/pkg-kafka/kafka"
	"github.com/sirupsen/logrus"
)

func NewClient(
	producerConfig map[string]interface{},
	consumerConfig map[string]interface{},
	serviceName string,
	log logrus.FieldLogger,
	prefix string,
) kafka.Client {
	logger.SetLogger(log)
	return logic.NewClient(producerConfig, consumerConfig, serviceName, prefix)
}

func Start(client kafka.Client) {
	log := logger.GetLogger()
	log.Info("START CONNECTING TO KAFKA")
	err := client.Start()
	if err != nil {
		log.Fatal(err, "can't start kafka client")
	}
}
