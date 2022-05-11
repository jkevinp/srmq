package main

import (
	"log"
	"time"

	srmq "github.com/jkevinp/srmq"
	"github.com/streadway/amqp"
)

const (
	rabbitConStr = "amqp://guest:guest@localhost:5672/"
)

func main() {
	go consumer()
	go publisher()
	select {}
}

func consumer() {

	consumer, err := srmq.NewConsumer(rabbitConStr, "gameQ", "game", srmq.EX_TYPE_DIRECT, false)

	if err != nil {
		panic(err)
	}

	if err := consumer.Connect(); err != nil {
		panic(err)
	}

	consumer.AddRouteFunc("match", func(d amqp.Delivery) {
		log.Println("match")
	})

	consumer.AddRouteFunc("odds", func(d amqp.Delivery) {
		log.Println("odds")
	})

	consumer.Listen()

	time.Sleep(5 * time.Second)
	consumer.StopListening()

	select {}
}

func publisher() {

	pub := srmq.NewPublisher(rabbitConStr, "game", srmq.EX_TYPE_DIRECT)

	quit := make(chan bool)

	interval := time.NewTicker(1 * time.Second)

	go func() {
		for {
			select {
			case <-interval.C:

				println("Publishing...")
				pub.Publish("match", "text/plain", []byte("match data"))

				pub.Publish("odds", "text/plain", []byte("odds"))

			case <-quit:
				interval.Stop()
				return
			}
		}
	}()

	<-quit

}
