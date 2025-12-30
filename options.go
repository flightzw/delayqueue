package delayqueue

import "time"

type QueueOption func(*DelayQueue)

func WithHashTagKey() QueueOption {
	return func(dq *DelayQueue) {
		dq.useHashTag = true
	}
}

func WithCustomPrefix(prefix string) QueueOption {
	return func(dq *DelayQueue) {
		dq.keyPrefix = prefix + ":" + dq.name
	}
}

// WithCallback set callback for queue to receives and consumes messages
// callback returns true to confirm successfully consumed, false to re-deliver this message
func WithCallback(callback CallbackFunc) QueueOption {
	return func(dq *DelayQueue) {
		dq.cb = callback
	}
}

// WithLogger customizes logger for queue
func WithLogger(logger Logger) QueueOption {
	return func(dq *DelayQueue) {
		dq.logger = logger
	}
}

// WithFetchInterval customizes the interval at which consumer fetch message from redis
func WithFetchInterval(d time.Duration) QueueOption {
	return func(dq *DelayQueue) {
		dq.fetchInterval = d
	}
}

// WithScriptPreload use script load command preload scripts to redis
func WithScriptPreload(flag bool) QueueOption {
	return func(dq *DelayQueue) {
		dq.scriptPreload = flag
	}
}

// WithMaxConsumeDuration customizes max consume duration
// If no acknowledge received within WithMaxConsumeDuration after message delivery, DelayQueue will try to deliver this message again
func WithMaxConsumeDuration(d time.Duration) QueueOption {
	return func(dq *DelayQueue) {
		dq.maxConsumeDuration = d
	}
}

// WithFetchLimit limits the max number of processing messages, 0 means no limit
func WithFetchLimit(limit uint) QueueOption {
	return func(dq *DelayQueue) {
		dq.fetchLimit = limit
	}
}

// WithConcurrent sets the number of concurrent consumers
func WithConcurrent(c uint) QueueOption {
	return func(dq *DelayQueue) {
		dq.concurrent = c
	}
}

// WithDefaultRetryCount customizes the max number of retry, it effects of messages in this queue
// use WithRetryCount during DelayQueue.SendScheduleMsg or DelayQueue.SendDelayMsg to specific retry count of particular message
func WithDefaultRetryCount(count uint) QueueOption {
	return func(dq *DelayQueue) {
		dq.defaultRetryCount = count
	}
}

// WithNackRedeliveryDelay customizes the interval between redelivery and nack (callback returns false)
// If consumption exceeded deadline, the message will be redelivered immediately
func WithNackRedeliveryDelay(d time.Duration) QueueOption {
	return func(dq *DelayQueue) {
		dq.nackRedeliveryDelay = d
	}
}

type pushOptions struct {
	msgID      string
	allowCover bool
	msgTTL     time.Duration
	retryCount uint
}

type PushOption func(*pushOptions)

func WithMsgID(id string, cover bool) PushOption {
	return func(po *pushOptions) {
		po.msgID = id
		po.allowCover = cover
	}
}

func WithMsgTTL(d time.Duration) PushOption {
	return func(po *pushOptions) {
		po.msgTTL = d
	}
}

func WithRetryCount(count uint) PushOption {
	return func(po *pushOptions) {
		po.retryCount = count
	}
}
