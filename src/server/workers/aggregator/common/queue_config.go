package common

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
