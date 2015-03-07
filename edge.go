package main

import (
	"./common"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
	"github.com/twinj/uuid"
)

var (
	url      = flag.String("u", "amqp://guest:guest@localhost:5672/", "The url to rabbitmq")
	queue    = flag.String("q", "mstest", "The queue to use")
	msgcount = flag.Int("c", 50, "Number of messages to send")
	qpm      = flag.Bool("qpm", false, "Add this to declare a new response queue per message")
	lpl      = flag.Bool("lpl", false, "Add this to log per line rather than just start and end")
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func declareResponseQueue(ch *amqp.Channel) (amqp.Queue, <-chan amqp.Delivery) {

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when usused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	if *lpl {
		log.Printf("Declared queue called %s", q.Name)
	}

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

	return q, msgs
}

func callRabbit() {
	log.Printf("Connecting to %s", *url)
	conn, err := amqp.Dial(*url)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	start := time.Now()

	var q amqp.Queue
	var msgs <-chan amqp.Delivery

	q, msgs = declareResponseQueue(ch)

	sum := 0
	for i := 0; i < *msgcount; i++ {
		callStart := time.Now()

		u := uuid.NewV4()
		corrId := u.String()

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

				dat := &common.HealthCheck{}

				if err := json.Unmarshal(d.Body, &dat); err != nil {
					panic(err)
				}
				callElapsed := time.Since(callStart)
				dat.SetMS("rabbitmq", callElapsed.Seconds()*1000)

				if *lpl {
					log.Printf("#%d : Message %s : Worker %s : Healthy = %t : Took %fms",
						i+1,
						corrId,
						dat.WorkerId,
						dat.Healthy,
						dat.ResponseMS["rabbitmq"])
				}

				break
			}
		}
		if *qpm {
			q, msgs = declareResponseQueue(ch)
		}
	}
	elapsed := time.Since(start)
	ms := elapsed.Seconds() * 1000
	log.Printf("Processed %d messages in %.2fms (%.2fms/msg)\n", *msgcount, ms, ms/float64(*msgcount))
	return
}

func main() {
	flag.Parse()

	log.Println("Starting...")

	callRabbit()

	log.Println("Finished.")
}
