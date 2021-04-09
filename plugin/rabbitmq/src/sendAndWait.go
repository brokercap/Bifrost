package src

import (
	"fmt"
	"github.com/streadway/amqp"
	"time"
)

func (This *Conn) SendAndWait(exchange *string, routingkey *string, c *[]byte, DeliveryMode uint8) (bool, error) {
	ch := This.getChannel(true)
	if ch == nil {
		This.status = "close"
		return false, This.err
	}
	err := ch.Publish(
		*exchange,   // exchange
		*routingkey, // routing key
		true,        // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         *c,
			DeliveryMode: DeliveryMode,
			Expiration:   This.p.expir,
		})
	if err != nil {
		This.err = err
		This.status = "close"
		return false, err
	}
	timer := time.NewTimer(10 * time.Second)
	select {
	case d := <-This.confirmWait:
		if d.DeliveryTag >= 0 {
			timer.Stop()
			return true, nil
		}
		This.err = fmt.Errorf("unkonw err")
		break
	case <-timer.C:
		This.err = fmt.Errorf("server no response")
		break
	}
	timer.Stop()
	This.status = "close"
	return false, This.err
}

func (This *Conn) SendAndNoWait(exchange *string, routingkey *string, c *[]byte, DeliveryMode uint8) (bool, error) {
	ch := This.getChannel(false)
	if ch == nil {
		This.status = "close"
		return false, This.err
	}
	err := ch.Publish(
		*exchange,   // exchange
		*routingkey, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         *c,
			DeliveryMode: DeliveryMode,
			Expiration:   This.p.expir,
		})
	if err != nil {
		This.err = err
		This.status = "close"
		return false, err
	}
	return true, nil
}
