package rabbit

import "github.com/streadway/amqp"

type RabbitMQ struct {
	conn *amqp.Connection
}

type EXCHANGE_TYPE string

const (
	EX_TYPE_DIRECT EXCHANGE_TYPE = "direct"
	EX_TYPE_FANOUT EXCHANGE_TYPE = "fanout"
	EX_TYPE_TOPIC  EXCHANGE_TYPE = "headers"
	EX_TYPE_HEADER EXCHANGE_TYPE = "topic"
)

func NewConnection(dsn string) (RabbitMQ, error) {

	conn, err := amqp.Dial(dsn)

	return RabbitMQ{conn}, err
}

func (rmq *RabbitMQ) GetChannel() (*amqp.Channel, error) {
	return rmq.conn.Channel()
}
