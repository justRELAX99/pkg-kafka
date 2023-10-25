package client

import (
	"context"
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
	"gitlab.enkod.tech/pkg/kafka/pkg/logger"
	"time"
)

const (
	// Время ожидания, пока очередь в буфере продусера переполнена
	queueFullWaitTime = time.Second * 5

	reconnectTime = time.Second * 10

	flushTimeout = 5000

	readTimeout = time.Second
)

type client struct {
	serviceName string
	topicPrefix string
	consumers   consumers
	producer    *producer
}

func newClient(
	producerConfig kafka.ConfigMap,
	consumerConfig kafka.ConfigMap,
	serviceName string,
	prefix string,
) Client {
	consumerConfig["group.id"] = serviceName
	return &client{
		serviceName: serviceName,
		producer:    newProducer(producerConfig),
		consumers:   newConsumers(consumerConfig),
		topicPrefix: prefix,
	}
}

func (c *client) Start() (err error) {
	err = c.producer.initProducer()
	if err != nil {
		return
	}
	if len(c.consumers.consumers) != 0 {
		err = c.producer.createTopics(c.consumers.getUniqByNameTopicSpecifications())
		if err != nil {
			return
		}
	}
	c.consumers.initConsumers()
	return
}

func (c *client) Pre(mw ...MiddlewareFunc) {
	for _, v := range mw {
		c.consumers.mwFuncs = append(c.consumers.mwFuncs, v)
	}
}

func (c *client) StopSubscribe() {
	c.consumers.stopConsumers()
}

func (c *client) StopProduce() {
	c.producer.stop()
}

func (c *client) Publish(ctx context.Context, topic string, data interface{}, headers ...map[string][]byte) (err error) {
	var dataB []byte
	if dataS, ok := data.(string); ok {
		dataB = []byte(dataS)
	} else {
		dataB, err = json.Marshal(data)
		if err != nil {
			return errors.Wrap(err, "cant marshal data")
		}
	}
	return c.publishByte(ctx, topic, dataB, headers...)
}

func (c *client) publishByte(ctx context.Context, topic string, data []byte, headers ...map[string][]byte) (err error) {
	message := NewMessage(topic, data, NewMessageHeaders(headers...), "")
	message.Topic = c.topicPrefix + message.Topic
	message.Headers.setServiceName(c.serviceName)
	return c.producer.publish(ctx, message)
}

func (c *client) Subscribe(h Handler, countConsumers int, specification Specifications) {
	log := logger.GetLogger()
	topicSpecification := NewTopicSpecifications(specification)
	topicSpecification.Topic = c.topicPrefix + topicSpecification.Topic
	for j := 0; j < countConsumers; j++ {
		err := c.consumers.addNewConsumer(h, topicSpecification)
		if err != nil {
			log.Fatal(err, "can't create new consumer")
		}
	}
}
func (c *client) PrePublish(f Pre) {
	c.producer.prePublish = append(c.producer.prePublish, f)
}