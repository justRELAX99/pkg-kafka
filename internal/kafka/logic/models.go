package logic

import (
	cKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pkg/errors"
)

const (
	serviceNameHeaderKey = "service_name"
)

func errToKafka(err error) (cKafka.Error, bool) {
	var kafkaErr cKafka.Error
	if err == nil {
		return kafkaErr, false
	}
	errors.As(err, &kafkaErr)
	return kafkaErr, true
}
