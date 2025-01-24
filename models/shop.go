package models

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

type Order struct {
	CustomerName string `json:"customer_name"`
	CoffeType    string `json:"coffe_type"`
}

func PushOrderToQueue(topic string, message []byte) error {
	brokers := []string{"localhost:9092"}

	// Create Connection
	producer, err := ConncectProducer(brokers)

	if err != nil {
		return err
	}

	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	// Send Message
	partition, offset, err := producer.SendMessage(msg)

	log.Printf("Order is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

	return nil
}

func ProcessOrder() {
	topic := "coffe_orders"
	msgCount := 0

	// Create a new consumer and start it
	worker, err := ConnectConsumer([]string{"localhost:9092"})
	if err != nil {
		panic(err)
	}

	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	fmt.Println("Consumer started")

	// Handle OS signals - used to stop the process
	signChan := make(chan os.Signal, 1)
	signal.Notify(signChan, syscall.SIGINT, syscall.SIGTERM)

	// Create a Go routine to run the consumer
	doneChan := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				fmt.Printf("Received order count: %d | Topic(%s) | Message(%s)\n", msgCount, msg.Topic, string(msg.Value))
				order := string(msg.Value)
				fmt.Printf("Brewing coffee for order: %s\n", order)
			case <-signChan:
				fmt.Println("Interrupt is detected")
				doneChan <- struct{}{}
			}
		}
	}()

	<-doneChan
	fmt.Println("Processed", msgCount, "messages")

	// Close the consumer on exit
	err = worker.Close()
	if err != nil {
		panic(err)
	}
}

func ConncectProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	return sarama.NewSyncProducer(brokers, config)
}

func ConnectConsumer(brokers []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	return sarama.NewConsumer(brokers, config)
}
