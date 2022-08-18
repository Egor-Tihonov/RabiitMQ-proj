package main

import (
	"log"

	"github.com/Egor-Tihonov/RabiitMQ-proj/internal/consumer"
	"github.com/Egor-Tihonov/RabiitMQ-proj/internal/repository"
)

func main() {
	cons, err := consumer.NewConsumer()
	if err != nil {
		log.Fatalf("error while creating consumer, %e", err)
	}
	defer func() {
		err = cons.CConn.Close()
		if err != nil {
			log.Fatalf("error, %e", err)
		}
	}()
	rps, err := repository.NewConnection()
	if err != nil {
		log.Fatalf("error while creating connection with DB")
	}
	defer rps.Pool.Close()
	err = cons.Read(rps)
	if err != nil {
		log.Fatalf("error: %e", err)
	}

}
