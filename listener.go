package delayqueue

// EventListener which will be called when events occur
// This Listener can be used to monitor running status
type EventListener interface {
	// OnEvent will be called when events occur
	OnEvent(*Event)
}

// pubsubListener receives events and reports them through redis pubsub for monitoring
type pubsubListener struct {
	redisCli   RedisCli
	reportChan string
}

func (l *pubsubListener) OnEvent(event *Event) {
	payload := encodeEvent(event)
	l.redisCli.Publish(l.reportChan, payload)
}
