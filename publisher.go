package sheduler

type Publisher interface {
	Publishing(SheduleData) error
}
