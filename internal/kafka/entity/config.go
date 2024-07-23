package entity

import (
	"github.com/CossackPyra/pyraconv"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Config map[string]interface{}

func (c Config) ToKafkaConfig() kafka.ConfigMap {
	kafkaConfig := make(kafka.ConfigMap, len(c))
	for k, v := range c {
		switch v.(type) {
		case int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64:
			v = int(pyraconv.ToInt64(v))
		}
		_ = kafkaConfig.SetKey(k, v)
	}
	return kafkaConfig
}

func (c Config) DefineLogChannel() {
	withLogs, _ := c["go.logs.channel.enable"].(bool)
	if !withLogs {
		return
	}
	c["go.logs.channel"] = make(chan kafka.LogEvent, 10000)
	if _, ok := c["debug"]; !ok {
		c["debug"] = "all"
	}
}

func (c Config) GetLogChannel() chan kafka.LogEvent {
	channel, _ := c["go.logs.channel"].(chan kafka.LogEvent)
	return channel
}
