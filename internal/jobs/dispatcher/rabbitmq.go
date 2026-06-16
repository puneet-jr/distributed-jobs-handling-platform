package dispatcher 

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
)


type RabbitMQDispatcher struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

func RabbitMQDispatcher(url string) (*RabbitMQDispatcher, error) {
	conn, err:= amqp.Dial(url)

	if err != nil {
		return nil, fmt.Errorf("Failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()

	if err!= nil {
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	// Dead letter Exchange

	if err := ch.ExchangeDeclare("jobs_dlx","direct",true,false,false,false,nil); err != nil {
		return nil, fmt.Errorf("failed to declare DLX: %w", err)
	}

}