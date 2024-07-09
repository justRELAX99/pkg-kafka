package logic

import (
	cKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/enkodio/pkg-kafka/internal/kafka/entity"
	"github.com/enkodio/pkg-kafka/internal/pkg/logger"
	"github.com/enkodio/pkg-kafka/kafka"
	"github.com/pkg/errors"
	"sync"
	"time"
)

type consumers struct {
	config    cKafka.ConfigMap
	consumers []*consumer
	mwFuncs   []kafka.MiddlewareFunc
	syncGroup *entity.SyncGroup
}

func newConsumers(config cKafka.ConfigMap) consumers {
	return consumers{
		config:    config,
		consumers: make([]*consumer, 0),
		syncGroup: entity.NewSyncGroup(),
	}
}

func (c *consumers) getUniqByNameTopicSpecifications() []kafka.TopicSpecifications {
	topicsMap := make(map[string]struct{}, len(c.consumers))
	topics := make([]kafka.TopicSpecifications, 0, len(c.consumers))

	for i := range c.consumers {
		if _, ok := topicsMap[c.consumers[i].Topic]; ok {
			continue
		}
		topicsMap[c.consumers[i].Topic] = struct{}{}
		topics = append(topics, c.consumers[i].TopicSpecifications)
	}
	return topics
}

func (c *consumers) addNewConsumer(handler kafka.Handler, topicSpecification kafka.TopicSpecifications) error {
	newConsumer := newConsumer(topicSpecification, handler)
	err := newConsumer.initConsumer(c.config)
	if err != nil {
		return errors.Wrap(err, "cant init kafka consumer")
	}
	c.consumers = append(c.consumers, newConsumer)
	return nil
}

func (c *consumers) createKafkaConsumers() error {
	for i := range c.consumers {
		err := c.consumers[i].initConsumer(c.config)
		if err != nil {
			return errors.Wrap(err, "cant init kafka consumer")
		}
	}
	return nil
}

func (c *consumers) stopConsumers() {
	c.syncGroup.Close()
	for i := range c.consumers {
		c.consumers[i].close()
	}
}

func (c *consumers) initConsumers() {
	once := &sync.Once{}
	c.syncGroup.NewDoneChan()
	// Запускаем каждого консумера в отдельной горутине
	for i := range c.consumers {
		c.syncGroup.Add(1)
		go func(consumer *consumer, syncGroup *entity.SyncGroup) {
			err := consumer.startConsume(syncGroup, c.mwFuncs)
			c.syncGroup.Done()
			if err != nil {
				once.Do(func() {
					c.reconnect()
				})
			}
		}(c.consumers[i], c.syncGroup)
	}
	c.syncGroup.Start()
	logger.GetLogger().Info("KAFKA CONSUMERS IS READY")
	return
}

func (c *consumers) reconnect() {
	log := logger.GetLogger()
	log.Debugf("start reconnecting consumers")
	// Стопаем консумеры
	c.stopConsumers()
	log.Debugf("consumers stopped")

	// Ждём 10 секунд для реконнекта
	time.Sleep(reconnectTime)

	// Запускаем новые консумеры
	for {
		err := c.createKafkaConsumers()
		if err != nil {
			logger.FromContext(nil).WithError(err).Error("cant init consumers")
			time.Sleep(reconnectTime)
			continue
		}
		log.Debugf("new consumers created")
		break
	}

	c.initConsumers()
}
