package qq

type broker interface {
}

// Broker for qq
type Broker struct {
}

// NewBroker is ctor for Broker
func NewBroker() *Broker {
	return &Broker{}
}
