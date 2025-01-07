package main

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	fmt.Println("Welcome to Kigali")

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "uatcons.crvs.nida.gov.rw:49092",
		"group.id":          "moh-crvs",
		"auto.offset.reset": "earliest",
		"security.protocol": "SASL_SSL",
		"sasl.mechanism":    "SCRAM-SHA-256",
		"sasl.username":     "moh-broker",
		"sasl.password":     "moh-broker-91823",
	})

	if err != nil {
		panic(err)
	}

	err = c.SubscribeTopics([]string{"death"}, nil)

	if err != nil {
		panic(err)
	}

	// A signal handler or similar could be used to set this to false to break the loop.
	run := true

	for run {
		msg, err := c.ReadMessage(time.Second)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	c.Close()

}
