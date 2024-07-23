package logic

import (
	"github.com/enkodio/pkg-kafka/internal/kafka/entity"
	"github.com/enkodio/pkg-kafka/internal/pkg/logger"
	"github.com/enkodio/pkg-kafka/kafka"
	"github.com/pkg/errors"
	"sync"
	"time"
)

type consumers struct {
	config    entity.Config
	consumers []*consumer
	mwFuncs   []kafka.MiddlewareFunc
	syncGroup *entity.SyncGroup
}

func newConsumers(config entity.Config) consumers {
	c := consumers{
		config:    config,
		consumers: make([]*consumer, 0),
		syncGroup: entity.NewSyncGroup(),
	}
	c.config.DefineLogChannel()
	return c
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
	newConsumer := newConsumer(topicSpecification, handler, c.config.ToKafkaConfig())
	err := newConsumer.initConsumer()
	if err != nil {
		return errors.Wrap(err, "cant init kafka consumer")
	}
	c.consumers = append(c.consumers, newConsumer)
	return nil
}

func (c *consumers) initConsumers() error {
	for i := range c.consumers {
		err := c.consumers[i].initConsumer()
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

func (c *consumers) startConsumers() {
	once := &sync.Once{}
	c.syncGroup.NewDoneChan()
	// Запускаем каждого консумера в отдельной горутине
	for i := range c.consumers {
		c.syncGroup.Add(1)
		go func(consumer *consumer, syncGroup *entity.SyncGroup) {
			err := consumer.startConsume(syncGroup, c.mwFuncs)
			c.syncGroup.Done()
			if err != nil {
				logger.GetLogger().WithError(err).Error("consuming error")
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
		err := c.initConsumers()
		if err != nil {
			log.WithError(err).Error("cant init consumers")
			time.Sleep(reconnectTime)
			continue
		}
		log.Debugf("new consumers created")
		break
	}

	c.startConsumers()
}
