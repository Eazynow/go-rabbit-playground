package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

var (
	url   = flag.String("url", "amqp://guest:guest@localhost:5672/", "The url to rabbitmq")
	queue = flag.String("queue", "sendrcv", "The queue to use")
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func main() {
	flag.Parse()

	log.Printf("Connecting to %s", *url)
	conn, err := amqp.Dial(*url)
	failOnError(err, "Failed to connect to RabbitMQ.")
	defer conn.Close()

	log.Println("Opening a channel")
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	log.Printf("Declaring a queue named %s", *queue)
	q, err := ch.QueueDeclare(
		*queue, // name
		false,  // durable
		false,  // delete when usused
		false,  // exclusive
		false,  // no-wait
		nil,    // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
