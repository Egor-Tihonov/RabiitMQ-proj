package main

import (
	"log"

	"github.com/Egor-Tihonov/RabiitMQ-proj/internal/producer"
)

const count = 2000

func main() {
	prod, err := producer.NewProducer()
	if err != nil {
		log.Fatalf("failed to connect with rabbitmq..., %e", err)
	}
	log.Println("Successfully create producer...")

	defer func() {
		err = prod.PCconn.Close()
		if err != nil {
			log.Fatalf("connection error, %e", err)
		}
	}()
	err = prod.Publish(count)
	if err != nil {
		log.Fatalf("error %e", err)
	}

}
