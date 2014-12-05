package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/streadway/amqp"
)

var (
	url      = flag.String("u", "amqp://guest:guest@localhost:5672/", "The url to rabbitmq")
	queue    = flag.String("q", "mstest", "The queue to use")
	msgcount = flag.Int("c", 50, "Number of messages to send")
)

type HealthResponse struct {
	Healthy bool `json:"healthy"`
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func callRabbit() {
	log.Printf("Connecting to %s", *url)
	conn, err := amqp.Dial(*url)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when usused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	log.Printf("Declared queue called %s", q.Name)

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

	corrId := randomString(32)

	start := time.Now()

	sum := 0
	for i := 0; i < *msgcount; i++ {
		callStart := time.Now()
		sum += i
		err = ch.Publish(
			"",     // exchange
			*queue, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType:   "text/plain",
				CorrelationId: corrId,
				ReplyTo:       q.Name,
				Body:          []byte(""),
			})
		failOnError(err, "Failed to publish a message")

		for d := range msgs {
			if corrId == d.CorrelationId {

				dat := &HealthResponse{}

				if err := json.Unmarshal(d.Body, &dat); err != nil {
					panic(err)
				}
				callElapsed := time.Since(callStart)
				log.Printf("#%d : Healthy = %t took %fms", i+1, dat.Healthy, callElapsed.Seconds())
				break
			}
		}
	}
	elapsed := time.Since(start)
	log.Printf("Processed %d messages in %fms\n", *msgcount, elapsed.Seconds()*1000)
	return
}

func main() {
	flag.Parse()

	rand.Seed(time.Now().UTC().UnixNano())

	log.Println("Starting...")

	callRabbit()

	log.Println("Finished.")
}

func bodyFrom(args []string) int {
	var s string
	if (len(args) < 2) || os.Args[1] == "" {
		s = "30"
	} else {
		s = strings.Join(args[1:], " ")
	}
	n, err := strconv.Atoi(s)
	failOnError(err, "Failed to convert arg to integer")
	return n
}
