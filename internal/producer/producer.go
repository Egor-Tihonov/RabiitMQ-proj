package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/Egor-Tihonov/RabiitMQ-proj/internal/models"
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

func (p *Producer) Publish(count int) error {
	var messages []amqp.Publishing
	for i := 1; i < 500; i++ {
		message := models.Message{Msg: "new massage"}
		msg, err := json.Marshal(message)
		if err != nil {
			break
		}
		messages = append(messages, amqp.Publishing{Body: msg})
	}
	ch, err := p.PCconn.Channel()
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
	for _, a := range messages {
		err = ch.PublishWithContext(
			context.Background(),
			"",
			"Test",
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        a.Body,
			},
		)
		if err != nil {
			log.Fatalf("failed to publish message..., %e", err)
			return err
		}
	}
	fmt.Println("Successfully publish message...")
	return nil
}
