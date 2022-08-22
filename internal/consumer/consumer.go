package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/Egor-Tihonov/RabiitMQ-proj/internal/models"
	"github.com/Egor-Tihonov/RabiitMQ-proj/internal/repository"
	"github.com/jackc/pgx/v4"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	CConn *amqp.Connection
}

func NewConsumer() (*Consumer, error) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		return nil, err
	}
	return &Consumer{CConn: conn}, nil
}

func (c *Consumer) Read(rps *repository.DB) error {
	pgxBatch := pgx.Batch{}
	ch, err := c.CConn.Channel()
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
	defer ch.Close()
	msg, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	go func(a *pgx.Batch) {
		for d := range msg {
			message := models.Message{}
			err = json.Unmarshal([]byte(d.Body), &message)
			if err != nil {
				log.Fatalf("internal/consumer: unmarshal error, %e", err)
			}
			a.Queue("insert into rablerabbir(msg) values($1)", message.Msg)
		}
	}(&pgxBatch)
	fmt.Println("Successfully conntect to rabbitmq instance...")
	fmt.Println("Wait for sending messaes to db...")
	err = rps.Write(context.Background(), &pgxBatch)
	if err != nil {
		return err
	}
	return nil
}
