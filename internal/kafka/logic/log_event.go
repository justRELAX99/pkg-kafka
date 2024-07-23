package logic

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/enkodio/pkg-kafka/internal/kafka/entity"
	"github.com/enkodio/pkg-kafka/internal/pkg/logger"
)

type logEvent struct {
	consumerLogEmitter chan kafka.LogEvent
	producerLogEmitter chan kafka.LogEvent
	producerSyncGroup  *entity.SyncGroup
	consumerSyncGroup  *entity.SyncGroup
}

func newLogEvent(
	consumerLogEmitter chan kafka.LogEvent,
	producerLogEmitter chan kafka.LogEvent,
) logEvent {
	return logEvent{
		consumerLogEmitter: consumerLogEmitter,
		producerLogEmitter: producerLogEmitter,
		producerSyncGroup:  entity.NewSyncGroup(),
		consumerSyncGroup:  entity.NewSyncGroup(),
	}
}

func handleLogEventChan(name string, logsChan chan kafka.LogEvent, syncGroup *entity.SyncGroup) {
	if logsChan == nil {
		return
	}
	log := logger.GetLogger()
	for {
		select {
		case <-syncGroup.IsDone():
			return
		case gotLog := <-logsChan:
			log.Infof("got log event for %s: %s", name, gotLog.String())
		}
	}
}

func (l logEvent) start() {
	l.producerSyncGroup.Add(1)
	go func() {
		handleLogEventChan("producer", l.producerLogEmitter, l.producerSyncGroup)
		l.producerSyncGroup.Done()
	}()
	l.consumerSyncGroup.Add(1)
	go func() {
		handleLogEventChan("consumer", l.consumerLogEmitter, l.consumerSyncGroup)
		l.consumerSyncGroup.Done()
	}()
}

func (l logEvent) stopProducer() {
	l.producerSyncGroup.Close()
}

func (l logEvent) stopConsumer() {
	l.consumerSyncGroup.Close()
}
