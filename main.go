package main

import (
	"errors"
	"fmt"
	"log"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"direct_queue",
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": "dl_queue",
		},
	)
	failOnError(err, "Failed to declare a queue")

	qDlq, err := ch.QueueDeclare(
		"dl_queue",
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": "direct_queue",
		},
	)
	failOnError(err, "Failed to declare a queue")

	qJunk, err := ch.QueueDeclare(
		"junk_queue",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare a queue")

	body := "OTP request payload"
	err = ch.Publish(
		"",
		q.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s", body)

	msgs, err := ch.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register a consumer")

	msgsDlq, err := ch.Consume(
		qDlq.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			count := CheckLimitRetry(d)
			log.Println("Try count", count)
			// check retry limit
			if count >= 3 {
				fmt.Println("do ack message")
				d.Ack(false)

				err = ch.Publish(
					"",
					qJunk.Name,
					false,
					false,
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        []byte(body),
					})
				failOnError(err, "Failed to publish a junk message")
				log.Printf(" [x] Sent to junk %s", body)
			} else {
				err := processOTPRequest(string(d.Body))
				if err != nil {
					log.Printf("Error processing message: %s", err)
					d.Reject(false)
				} else {
					d.Ack(false) // Acknowledge the message
				}
			}
		}
	}()

	go func() {
		for d := range msgsDlq {
			log.Printf("Received a message in DLQ: %s", d.Body)

			failOnError(err, "Failed to re-publish message to direct queue")
			//d.Ack(false) // Acknowledge the message to remove it from the DLQ
			d.Reject(false)
		}
	}()
	<-forever
}

func CheckLimitRetry(msg amqp.Delivery) int64 {

	// Read the "x-death" property from the message header:
	xDeath, ok := msg.Headers["x-death"].([]interface{})
	if !ok || len(xDeath) == 0 {
		return 0
	}

	// Get the "x-death" information from the first element:
	firstDeathInfo, ok := xDeath[0].(amqp.Table)
	if !ok {
		log.Println("Unable to decipher 'x-death' information.")
		return 0
	}

	return firstDeathInfo["count"].(int64)
}

func processOTPRequest(body string) error {
	// Simulate OTP processing
	log.Printf("Processing OTP request: %s", body)
	time.Sleep(5 * time.Second) // Simulate processing time
	return errors.New("something wrong") // simulate the error process
}
