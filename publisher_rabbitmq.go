package sheduler

import "errors"

var (
	_ Publisher = (*RabbitMQPublisher)(nil)
)

func NewRabbitMQPublisher() Publisher {
	return &RabbitMQPublisher{}
}

type RabbitMQPublisher struct {
}

func (p *RabbitMQPublisher) Publishing(SheduleData) error {
	return errors.New("not implemented")
}
