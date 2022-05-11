package rabbit

import (
	"context"

	"github.com/streadway/amqp"
)

type consumer struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	q            *amqp.Queue
	exchangeName string
	exchangeType EXCHANGE_TYPE
	routeFunc    map[string]func(d amqp.Delivery)
	conStr       string
	isExclusive  bool
	queueName    string
	cancel       context.CancelFunc
	context      context.Context
}

func NewConsumer(dsn string, queueName, exchangeName string, exchangeType EXCHANGE_TYPE, isExclusive bool) (*consumer, error) {
	ctx, cancel := context.WithCancel(context.Background())
	return &consumer{
		conStr:       dsn,
		exchangeName: exchangeName,
		exchangeType: exchangeType,
		routeFunc:    make(map[string]func(d amqp.Delivery)),
		isExclusive:  isExclusive,
		queueName:    queueName,
		cancel:       cancel,
		context:      ctx,
	}, nil

}

//connect
func (c *consumer) Connect() error {

	conn, err := amqp.Dial(c.conStr)

	if err != nil {
		return err
	}

	ch, err := conn.Channel()

	if err != nil {
		return err
	}

	q, err := ch.QueueDeclare(
		c.queueName,
		false,
		false,
		c.isExclusive,
		false,
		nil,
	)

	if err != nil {
		return err
	}

	err = ch.ExchangeDeclare(
		c.exchangeName,
		string(c.exchangeType),
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return err
	}

	c.conn = conn
	c.channel = ch
	c.q = &q

	return nil

}

func (c *consumer) AddRouteFunc(routeKey string, f func(d amqp.Delivery)) error {

	err := c.channel.QueueBind(
		c.queueName,
		"",
		c.exchangeName,
		false,
		nil,
	)

	if err != nil {
		return err
	}

	c.routeFunc[routeKey] = f

	return nil
}

//start listening
func (c *consumer) Listen() {

	m := make(map[string]<-chan amqp.Delivery)

	for routeKey, _ := range c.routeFunc {

		msgs, err := c.channel.Consume(
			c.queueName,
			"",
			c.isExclusive,
			false,
			false,
			false,
			nil,
		)

		if err != nil {
			panic(err)
		}
		m[routeKey] = msgs
	}

	for routeKey, messages := range m {
		go func(routeKey string, messages <-chan amqp.Delivery) {

			for message := range messages {
				select {
				case <-c.context.Done():
					println("quitting listener", routeKey)
					return
				default:
					if handler, ok := c.routeFunc[routeKey]; ok {
						handler(message)
					} else {
						println("no handler for routeKey:", routeKey)
					}
				}
			}

		}(routeKey, messages)
	}

}

func (c *consumer) StopListening() {
	println("stop listening requested")
	// c.quit <- true
	c.cancel()
}

//stop listening
func (c *consumer) Close() {
	c.StopListening()
	c.channel.Close()
	c.conn.Close()

}
