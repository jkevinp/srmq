package smrq

import "github.com/streadway/amqp"

type directPublisher struct {
	exchangeName string
	channel      *amqp.Channel
	rabbit       *RabbitMQ
}

func NewPublisher(dsn string, exchangeName string, exchangeType EXCHANGE_TYPE) *directPublisher {
	conn, err := NewConnection(dsn)
	if err != nil {
		panic(err)
	}

	ch, err := conn.GetChannel()

	if err != nil {
		panic(err)
	}

	ch.ExchangeDeclare(
		exchangeName,
		string(exchangeType),
		true,
		false,
		false,
		false,
		nil,
	)

	return &directPublisher{
		exchangeName: exchangeName,
		channel:      ch,
		rabbit:       &conn,
	}
}

func (pub *directPublisher) Publish(routeKey string, contentType string, data []byte) error {
	err := pub.channel.Publish(
		pub.exchangeName,
		routeKey,
		false,
		false,
		amqp.Publishing{
			ContentType: contentType,
			Body:        data,
		},
	)

	if err != nil {
		return err
	}

	return nil

}
