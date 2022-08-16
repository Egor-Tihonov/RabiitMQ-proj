package producer

import (
	"context"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer struct {
	PCconn *amqp.Connection
}

func NewProducer() (*Producer, error) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		return nil, err
	}

	return &Producer{PCconn: conn}, nil
}

func (p *Producer) Publish(conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	q, err := ch.QueueDeclare(
		"Test",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	fmt.Println(q)
	err = ch.PublishWithContext(
		context.Background(),
		"",
		"Test",
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte("hello world!"),
		},
	)
	if err != nil {
		log.Fatalf("failed to publish message..., %e", err)
	}
	fmt.Println("Successfully publish message...")
	return nil
}
