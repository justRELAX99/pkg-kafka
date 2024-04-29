package entity

import (
	cKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Settings struct {
	KafkaProducer cKafka.ConfigMap `json:"kafkaProducer"`
	KafkaConsumer cKafka.ConfigMap `json:"kafkaConsumer"`
}
