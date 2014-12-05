package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

var (
	url = flag.String("url", "amqp://guest:guest@localhost:5672/", "The url to rabbitmq")
	msg = flag.String("message", "hi there", "The message to send")
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func main() {

	sendqueue := "hello"
	body := "hi there"

	flag.Parse()
	log.Printf("Connecting to %s", *url)
	conn, err := amqp.Dial(*url)
	failOnError(err, "Failed to connect to RabbitMQ.")
	defer conn.Close()

	log.Println("Opening a channel")
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	log.Printf("Declaring a queue named %s", sendqueue)
	q, err := ch.QueueDeclare(
		sendqueue, // name
		false,     // durable
		false,     // delete when usused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue")

	log.Printf("Sending message: %s", sendqueue)
	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish a message")

	log.Println("Finished.")
}
