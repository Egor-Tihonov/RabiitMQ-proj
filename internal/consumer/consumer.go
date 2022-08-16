package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v4/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conndb, err := pgxpool.Connect(context.Background(), "postgresql://postgres:123@localhost:5432/postgres")
	if err != nil {
		log.Fatalf("%e", err)
	}
	fmt.Println("Consummer...")
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("failed to connect with rabbitmq..., %e", err)
	}
	defer conn.Close()
	log.Println("Successfully connected to rabbitmq...")
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("%e", err)
	}
	defer ch.Close()
	msg, err := ch.Consume(
		"Test",
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("%e", err)
	}
	forever := make(chan bool)
	go func() {
		for d := range msg {
			fmt.Printf("Receiver messages: %s\n", d.Body)
			_, err = conndb.Exec(context.Background(), "insert into tablerabbit(message) values($1)", string(d.Body))
			if err != nil {
				log.Printf("error %e", err)
			}
		}
	}()
	fmt.Println("Successfully conntect to rabbitmq instance...")
	fmt.Println("Wait for messages...")
	<-forever
}
