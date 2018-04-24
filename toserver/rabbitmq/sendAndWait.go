package rabbitmq

import (
	"github.com/streadway/amqp"
	"time"
	"fmt"
)

func (This *Conn) SendAndWait(exchange *string,routingkey *string, c *[]byte,DeliveryMode *uint8) (bool,error) {
	err := This.chWait.Publish(
		*exchange,     // exchange
		*routingkey, // routing key
		true,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: 	"text/plain",
			Body:   *c,
			DeliveryMode:	*DeliveryMode,
			Expiration:	This.expir,
		})
	if err != nil{
		This.err = err
		This.status = "close"
		return false,err
		}
	select {
	case d := <-This.confirmWait:
		if d.DeliveryTag >= 0{
			return true,nil
		}
		This.err = fmt.Errorf("unkonw err")
		break
	case <-time.After(10 * time.Second):
		This.err = fmt.Errorf("server no response")
		break
	}
	This.status = "close"
	return false,This.err
}

func (This *Conn) SendAndNoWait(exchange *string,routingkey *string, c *[]byte,DeliveryMode *uint8) (bool,error) {
	err := This.ch.Publish(
		*exchange,     // exchange
		*routingkey, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: 	"text/plain",
			Body:   *c,
			DeliveryMode:	*DeliveryMode,
			Expiration:	This.expir,
		})
	if err != nil{
		This.err = err
		This.status = "close"
		return false,err
	}
	return true,nil
}