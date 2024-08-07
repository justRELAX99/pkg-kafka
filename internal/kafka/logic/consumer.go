package logic

import (
	"context"
	cKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/enkodio/pkg-kafka/internal/kafka/entity"
	"github.com/enkodio/pkg-kafka/internal/pkg/logger"
	"github.com/enkodio/pkg-kafka/kafka"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

type consumer struct {
	handler kafka.Handler
	config  cKafka.ConfigMap
	kafka.TopicSpecifications
	*cKafka.Consumer
}

func newConsumer(
	topicSpecifications kafka.TopicSpecifications,
	handler kafka.Handler,
	config cKafka.ConfigMap,
) *consumer {
	c := &consumer{
		TopicSpecifications: topicSpecifications,
		handler:             handler,
		config:              config,
	}
	if topicSpecifications.WithUniqGroupId {
		_ = c.config.SetKey("group.id", uuid.New().String())
	}
	_ = c.config.SetKey("client.id", uuid.New().String())
	return c
}

func (c *consumer) initConsumer() error {
	// Создаём консумера
	kafkaConsumer, err := cKafka.NewConsumer(&c.config)
	if err != nil {
		return errors.Wrap(err, "cant create kafka consumer")
	}
	// Подписываем консумера на топик
	err = kafkaConsumer.Subscribe(c.Topic, c.getRebalanceCb())
	if err != nil {
		return errors.Wrap(err, "cant subscribe kafka consumer")
	}
	c.Consumer = kafkaConsumer
	return nil
}

func (c *consumer) getRebalanceCb() cKafka.RebalanceCb {
	return func(c *cKafka.Consumer, event cKafka.Event) error {
		logger.GetLogger().Infof("Rebalanced: %v; rebalanced protocol: %v;",
			event.String(),
			c.GetRebalanceProtocol())
		return nil
	}
}

func (c *consumer) startConsume(syncGroup *entity.SyncGroup, mwFuncs []kafka.MiddlewareFunc) error {
	log := logger.GetLogger()
	var handler kafka.MessageHandler = func(ctx context.Context, message kafka.Message) error {
		return c.handler(ctx, message.GetBody())
	}

	// Прогоняем хендлер через миддлверы
	for j := len(mwFuncs) - 1; j >= 0; j-- {
		handler = mwFuncs[j](handler)
	}
	for {
		select {
		case <-syncGroup.IsDone():
			return nil
		default:
			msg, err := c.ReadMessage(readTimeout)
			if kafkaErr, ok := errToKafka(err); ok {
				if kafkaErr.Code() == cKafka.ErrTimedOut || kafkaErr.IsRetriable() {
					continue
				}
				return errors.Wrap(err, "cant read kafka message")
			}
			if msg.TopicPartition.Error != nil {
				log.WithError(msg.TopicPartition.Error).Error("err in consumed message")
			}
			err = handler(context.Background(), entity.FromConsumerMessage(msg))
			if err != nil && c.CheckError {
				log.WithError(err).Debug("try to read message again")
				c.rollbackConsumerTransaction(msg.TopicPartition)
			}
		}
	}
}

func (c *consumer) rollbackConsumerTransaction(topicPartition cKafka.TopicPartition) {
	// В committed лежит массив из одного элемента, потому что передаём одну партицию, которую нужно сбросить
	committed, err := c.Committed([]cKafka.TopicPartition{{Topic: &c.Topic, Partition: topicPartition.Partition}}, -1)
	log := logger.GetLogger()
	if err != nil {
		log.Error(err)
		return
	}
	if committed[0].Offset < 0 {
		committed[0].Offset = cKafka.OffsetBeginning
	} else {
		committed[0].Offset = topicPartition.Offset
	}
	err = c.Seek(committed[0], 0)
	if err != nil {
		log.Error(err)
		return
	}
	return
}

func (c *consumer) close() {
	log := logger.GetLogger()

	_, err := c.Commit()
	if kafkaErr, ok := errToKafka(err); ok && kafkaErr.Code() != cKafka.ErrNoOffset {
		log.WithError(err).Errorf("cant commit offset for topic: %s", err.Error())
	}
	// Отписка от назначенных топиков
	err = c.Unsubscribe()
	if err != nil {
		log.WithError(err).Errorf("cant unsubscribe connection: %s", err.Error())
	}
	// Закрытие соединения
	err = c.Close()
	if err != nil {
		log.WithError(err).Errorf("cant close consumer connection: %s", err.Error())
	}
}
