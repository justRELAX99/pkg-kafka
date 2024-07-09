package entity

type Settings struct {
	KafkaProducer map[string]interface{} `json:"kafkaProducer"`
	KafkaConsumer map[string]interface{} `json:"kafkaConsumer"`
}
