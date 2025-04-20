package common

const DUMMY_QUEUE_NAME = "DUMMY"

type QueueConfig struct {
	DeleteWhenUnused bool
	Durable          bool
	Exclusive        bool
	Name             string
	NoWait           bool
}

// Returns new queue config
func NewQueueConfig(
	deleteWhenUnused bool,
	durable bool,
	exclusive bool,
	name string,
	noWait bool) *QueueConfig {
	config := &QueueConfig{
		DeleteWhenUnused: deleteWhenUnused,
		Durable:          durable,
		Exclusive:        exclusive,
		Name:             name,
		NoWait:           noWait,
	}
	return config
}

func DummyQueueConfig() *QueueConfig {
	return NewQueueConfig(false, true, false, DUMMY_QUEUE_NAME, false)
}
