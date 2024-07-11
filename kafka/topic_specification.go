package kafka

const (
	// Значение реплик каждой партиции по умолчанию
	defaultReplicationFactor = 1
	// Значение партиций для топика по умолчанию
	defaultNumPartitions = 3
	// Максимальное количество реплик каждой партиции (равно количеству брокеров в кластере)
	maxReplicationFactor = 3
)

type Specifications interface {
	GetTopic() string
	GetNumPartitions() int
	GetReplicationFactor() int
	GetCheckError() bool
	GetWithUniqGroupId() bool
}

type TopicSpecifications struct {
	Topic             string
	NumPartitions     int
	ReplicationFactor int
	CheckError        bool
	WithUniqGroupId   bool
}

func NewTopicSpecifications(specifications Specifications) TopicSpecifications {
	return TopicSpecifications{
		Topic:             specifications.GetTopic(),
		NumPartitions:     specifications.GetNumPartitions(),
		ReplicationFactor: specifications.GetReplicationFactor(),
		CheckError:        specifications.GetCheckError(),
		WithUniqGroupId:   specifications.GetWithUniqGroupId(),
	}
}

func (t *TopicSpecifications) GetTopic() string {
	return t.Topic
}

func (t *TopicSpecifications) GetNumPartitions() int {
	if t.NumPartitions == 0 {
		return defaultNumPartitions
	}
	return t.NumPartitions
}

func (t *TopicSpecifications) GetReplicationFactor() int {
	if t.ReplicationFactor == 0 {
		return defaultReplicationFactor
	}
	if t.ReplicationFactor > maxReplicationFactor {
		return maxReplicationFactor
	}
	return t.ReplicationFactor
}

func (t *TopicSpecifications) GetCheckError() bool {
	return t.CheckError
}

func (t *TopicSpecifications) GetWithUniqGroupId() bool {
	return t.WithUniqGroupId
}
