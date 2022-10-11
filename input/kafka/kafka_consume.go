package kafka

import "github.com/Shopify/sarama"

func (c *Input) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Input) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Input) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select{
			

		}
	}
	return nil
}
