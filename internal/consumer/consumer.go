package consumer

import (
	"context"
	"encoding/json"
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
	log.Println("create conn with rabbitmq")
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
	i := 0
	for d := range msg {
		message := models.Message{}
		err = json.Unmarshal(d.Body, &message)
		if err != nil {
			log.Fatalf("internal/consumer: unmarshal error, %e", err)
			return err
		}
		pgxBatch.Queue("insert into tablerabbit(message) values($1)", message.Msg)
		i++
		if i == 1999 {
			err = ch.Close()
			if err != nil {
				return err
			}
		}
	}

	log.Println("Successfully conntect to rabbitmq instance...")
	log.Println("Wait for sending messaes to db...")
	// ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	// defer cancel()
	err = rps.Write(context.Background(), &pgxBatch)
	if err != nil {
		return err
	}
	return nil
}
