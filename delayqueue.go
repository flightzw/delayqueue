package delayqueue

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const (
	StatePending    = "pending"
	StateReady      = "ready"
	StateReadyRetry = "ready_to_retry"
	StateConsuming  = "consuming"
	StateUnknown    = "unknown"
)

// NilErr represents redis nil
var NilErr = errors.New("nil")

// CallbackFunc receives and consumes messages
// returns true to confirm successfully consumed, false to re-deliver this message
type CallbackFunc = func(string) bool

// RedisCli is abstraction for redis client, required commands only not all commands
type RedisCli interface {
	// Eval sends lua script to redis
	// args should be string, integer or float
	// returns string, int64, []interface{} (elements can be string or int64)
	Eval(script string, keys []string, args []interface{}) (interface{}, error)
	Set(key string, value string, expiration time.Duration) error
	// Get represents redis command GET
	// please NilErr when no such key in redis
	Get(key string) (string, error)
	Exists(key string) (int64, error)
	Del(keys []string) error
	HSet(key string, field string, value string) error
	HDel(key string, fields []string) error
	SMembers(key string) ([]string, error)
	SRem(key string, members []string) error
	ZAdd(key string, values map[string]float64) error
	ZRem(key string, fields []string) (int64, error)
	ZCard(key string) (int64, error)
	ZScore(key string, member string) (float64, error)
	LLen(key string) (int64, error)
	LRem(key string, count int64, value string) (int64, error)

	// Publish used for monitor only
	Publish(channel string, payload string) error
	// Subscribe used for monitor only
	// returns: payload channel, subscription closer, error; the subscription closer should close payload channel as well
	Subscribe(channel string) (payloads <-chan string, close func(), err error)

	// ScriptLoad call `script load` command
	ScriptLoad(script string) (string, error)
	// EvalSha run preload scripts
	// If there is no preload scripts please return error with message "NOSCRIPT"
	EvalSha(sha1 string, keys []string, args []interface{}) (interface{}, error)
}

// Logger is an abstraction of logging system
type Logger interface {
	Printf(format string, v ...interface{})
}

// DelayQueue is a message queue supporting delayed/scheduled delivery based on redis
type DelayQueue struct {
	redisKeys
	// name for this Queue. Make sure the name is unique in redis database
	name               string
	redisCli           RedisCli
	cb                 func(string) bool
	keyPrefix          string
	useHashTag         bool
	ticker             *time.Ticker
	logger             Logger
	close              chan struct{}
	running            int32
	maxConsumeDuration time.Duration // default 5 seconds
	msgTTL             time.Duration // default 1 hour
	defaultRetryCount  uint          // default 3
	fetchInterval      time.Duration // default 1 second
	fetchLimit         uint          // default no limit
	fetchCount         int32         // actually running task number
	concurrent         uint          // default 1, executed serially
	sha1map            map[string]string
	sha1mapMu          *sync.RWMutex
	scriptPreload      bool
	// for batch consume
	consumeBuffer chan string

	eventListener       EventListener
	nackRedeliveryDelay time.Duration
}

type redisKeys struct {
	msgKeyPrefix  string // string
	pendingKey    string // sorted set: message id -> delivery time
	readyKey      string // list
	unAckKey      string // sorted set: message id -> retry time
	retryKey      string // list
	retryCountKey string // hash: message id -> remain retry count
	garbageKey    string // set: message id
}

// NewQueue0 creates a new queue, use DelayQueue.StartConsume to consume or DelayQueue.SendScheduleMsg to publish message
// callback returns true to confirm successful consumption. If callback returns false or not return within maxConsumeDuration, DelayQueue will re-deliver this message
func NewQueue0(name string, cli RedisCli, opts ...QueueOption) (*DelayQueue, error) {
	if name == "" {
		return nil, errors.New("name is required")
	}
	if cli == nil {
		return nil, errors.New("cli is required")
	}
	queue := newDelayQueue(name, cli)
	for _, o := range opts {
		o(queue)
	}
	queue.keyPrefix = queue.keyPrefix + ":" + name
	if queue.useHashTag {
		queue.keyPrefix = "{" + queue.keyPrefix + "}"
	}
	queue.redisKeys = redisKeys{
		msgKeyPrefix:  queue.keyPrefix + ":msg:",
		pendingKey:    queue.keyPrefix + ":pending",
		readyKey:      queue.keyPrefix + ":ready",
		unAckKey:      queue.keyPrefix + ":unack",
		retryKey:      queue.keyPrefix + ":retry",
		retryCountKey: queue.keyPrefix + ":retry:cnt",
		garbageKey:    queue.keyPrefix + ":garbage",
	}
	return queue, nil
}

func newDelayQueue(name string, cli RedisCli) *DelayQueue {
	return &DelayQueue{
		name:                name,
		redisCli:            cli,
		keyPrefix:           "dp",
		useHashTag:          false,
		ticker:              &time.Ticker{},
		running:             0,
		maxConsumeDuration:  5 * time.Second,
		msgTTL:              time.Hour,
		logger:              log.Default(),
		defaultRetryCount:   3,
		fetchInterval:       time.Second,
		concurrent:          1,
		sha1map:             make(map[string]string),
		sha1mapMu:           &sync.RWMutex{},
		scriptPreload:       true,
		fetchLimit:          0,
		fetchCount:          0,
		consumeBuffer:       make(chan string),
		eventListener:       nil,
		nackRedeliveryDelay: 0,
	}
}

func (q *DelayQueue) RegisterCallback(fn CallbackFunc) *DelayQueue {
	q.cb = fn
	return q
}

func (q *DelayQueue) genMsgKey(idStr string) string {
	return q.msgKeyPrefix + idStr
}

func (q *DelayQueue) parsePushOptions(opts ...PushOption) *pushOptions {
	params := &pushOptions{
		retryCount: q.defaultRetryCount,
		msgTTL:     q.msgTTL,
	}
	for _, opt := range opts {
		opt(params)
	}
	return params
}

// SendScheduleMsgV2 submits a message delivered at given time
func (q *DelayQueue) SendScheduleMsgV2(payload string, t time.Time, opts ...PushOption) (*MessageInfo, error) {
	var (
		params = q.parsePushOptions(opts...) // parse options
		now    = time.Now()
	)
	// generate id
	if params.msgID == "" {
		params.msgID = uuid.Must(uuid.NewRandom()).String()
	}
	msgKey := q.genMsgKey(params.msgID)
	if params.allowCover {
		state, err := q.redisCli.Exists(msgKey)
		if err != nil {
			return nil, fmt.Errorf("check msg id failed: %v", err)
		}
		if state == 1 {
			if res, _ := q.TryIntercept(&MessageInfo{id: params.msgID}); !res.Intercepted {
				params.msgID = params.msgID + ""
			}
		}

	}
	// store msg
	msgTTL := t.Sub(now) + q.msgTTL // delivery + q.msgTTL
	err := q.redisCli.Set(msgKey, payload, msgTTL)
	if err != nil {
		return nil, fmt.Errorf("store msg failed: %v", err)
	}
	// store retry count
	err = q.redisCli.HSet(q.retryCountKey, params.msgID, strconv.Itoa(int(params.retryCount)))
	if err != nil {
		return nil, fmt.Errorf("store retry count failed: %v", err)
	}
	// put to pending
	err = q.redisCli.ZAdd(q.pendingKey, map[string]float64{params.msgID: float64(t.Unix())})
	if err != nil {
		return nil, fmt.Errorf("push to pending failed: %v", err)
	}
	q.reportEvent(NewMessageEvent, 1)
	return &MessageInfo{id: params.msgID}, nil
}

// SendDelayMsg submits a message delivered after given duration
func (q *DelayQueue) SendDelayMsgV2(payload string, duration time.Duration, opts ...PushOption) (*MessageInfo, error) {
	t := time.Now().Add(duration)
	return q.SendScheduleMsgV2(payload, t, opts...)
}

// SendScheduleMsg submits a message delivered at given time
// It is compatible with SendScheduleMsgV2, but does not return MessageInfo
func (q *DelayQueue) SendScheduleMsg(payload string, t time.Time, opts ...PushOption) error {
	_, err := q.SendScheduleMsgV2(payload, t, opts...)
	return err
}

// SendDelayMsg submits a message delivered after given duration
// It is compatible with SendDelayMsgV2, but does not return MessageInfo
func (q *DelayQueue) SendDelayMsg(payload string, duration time.Duration, opts ...PushOption) error {
	t := time.Now().Add(duration)
	return q.SendScheduleMsg(payload, t, opts...)
}

// TryIntercept trys to intercept a message
func (q *DelayQueue) TryIntercept(msg *MessageInfo) (*InterceptResult, error) {
	id := msg.ID()
	// try to intercept at ready
	removed, err := q.redisCli.LRem(q.readyKey, 0, id)
	if err != nil {
		q.logger.Printf("intercept %s from ready failed: %v", id, err)
	}
	if removed > 0 {
		_ = q.redisCli.Del([]string{q.genMsgKey(id)})
		_ = q.redisCli.HDel(q.retryCountKey, []string{id})
		return &InterceptResult{
			Intercepted: true,
			State:       StateReady,
		}, nil
	}
	// try to intercept at pending
	removed, err = q.redisCli.ZRem(q.pendingKey, []string{id})
	if err != nil {
		q.logger.Printf("intercept %s from pending failed: %v", id, err)
	}
	if removed > 0 {
		_ = q.redisCli.Del([]string{q.genMsgKey(id)})
		_ = q.redisCli.HDel(q.retryCountKey, []string{id})
		return &InterceptResult{
			Intercepted: true,
			State:       StatePending,
		}, nil
	}
	// message may be being consumed or has been successfully consumed
	// if the message has been successfully consumed, the following action will cause nothing
	// if the message is being consumed，the following action will prevent it from being retried
	q.redisCli.HDel(q.retryCountKey, []string{id})
	q.redisCli.LRem(q.retryKey, 0, id)

	return &InterceptResult{
		Intercepted: false,
		State:       StateUnknown,
	}, nil
}

// TryInterceptByID trys to intercept a message, wrap TryIntercept
func (q *DelayQueue) TryInterceptByID(msgid string) (*InterceptResult, error) {
	exists, err := q.redisCli.Exists(q.genMsgKey(msgid))
	if err != nil {
		return nil, err
	}
	if exists == 0 {
		return &InterceptResult{Intercepted: true, State: StateConsuming}, nil
	}
	return q.TryIntercept(&MessageInfo{id: msgid})
}

func (q *DelayQueue) loadScript(script string) (string, error) {
	sha1, err := q.redisCli.ScriptLoad(script)
	if err != nil {
		return "", err
	}
	q.sha1mapMu.Lock()
	q.sha1map[script] = sha1
	q.sha1mapMu.Unlock()
	return sha1, nil
}

func (q *DelayQueue) eval(script string, keys []string, args []interface{}) (interface{}, error) {
	if !q.scriptPreload {
		return q.redisCli.Eval(script, keys, args)
	}
	var err error
	q.sha1mapMu.RLock()
	sha1, ok := q.sha1map[script]
	q.sha1mapMu.RUnlock()
	if !ok {
		sha1, err = q.loadScript(script)
		if err != nil {
			return nil, err
		}
	}
	result, err := q.redisCli.EvalSha(sha1, keys, args)
	if err == nil {
		return result, err
	}
	// script not loaded, reload it
	// It is possible to access a node in the cluster that has no pre-loaded scripts.
	if strings.HasPrefix(err.Error(), "NOSCRIPT") {
		sha1, err = q.loadScript(script)
		if err != nil {
			return nil, err
		}
		// try again
		result, err = q.redisCli.EvalSha(sha1, keys, args)
	}
	return result, err
}

func (q *DelayQueue) pending2Ready() error {
	now := time.Now().Unix()
	keys := []string{q.pendingKey, q.readyKey}
	raw, err := q.eval(pending2ReadyScript, keys, []interface{}{now})
	if err != nil && err != NilErr {
		return fmt.Errorf("pending2ReadyScript failed: %v", err)
	}
	count, ok := raw.(int64)
	if ok {
		q.reportEvent(ReadyEvent, int(count))
	}
	return nil
}

func (q *DelayQueue) ready2Unack() (string, error) {
	retryTime := time.Now().Add(q.maxConsumeDuration).Unix()
	keys := []string{q.readyKey, q.unAckKey}
	ret, err := q.eval(ready2UnackScript, keys, []interface{}{retryTime})
	if err == NilErr {
		return "", err
	}
	if err != nil {
		return "", fmt.Errorf("ready2UnackScript failed: %v", err)
	}
	str, ok := ret.(string)
	if !ok {
		return "", fmt.Errorf("illegal result: %#v", ret)
	}
	q.reportEvent(DeliveredEvent, 1)
	return str, nil
}

func (q *DelayQueue) retry2Unack() (string, error) {
	retryTime := time.Now().Add(q.maxConsumeDuration).Unix()
	keys := []string{q.retryKey, q.unAckKey}
	ret, err := q.eval(ready2UnackScript, keys, []interface{}{retryTime, q.retryKey, q.unAckKey})
	if err == NilErr {
		return "", NilErr
	}
	if err != nil {
		return "", fmt.Errorf("ready2UnackScript failed: %v", err)
	}
	str, ok := ret.(string)
	if !ok {
		return "", fmt.Errorf("illegal result: %#v", ret)
	}
	return str, nil
}

func (q *DelayQueue) callback(idStr string) error {
	payload, err := q.redisCli.Get(q.genMsgKey(idStr))
	if err == NilErr {
		return nil
	}
	if err != nil {
		// Is an IO error?
		return fmt.Errorf("get message payload failed: %v", err)
	}
	ack := q.cb(payload)
	if ack {
		err = q.ack(idStr)
	} else {
		err = q.nack(idStr)
	}
	return err
}

func (q *DelayQueue) ack(idStr string) error {
	atomic.AddInt32(&q.fetchCount, -1)
	_, err := q.redisCli.ZRem(q.unAckKey, []string{idStr})
	if err != nil {
		return fmt.Errorf("remove from unack failed: %v", err)
	}
	// msg key has ttl, ignore result of delete
	_ = q.redisCli.Del([]string{q.genMsgKey(idStr)})
	_ = q.redisCli.HDel(q.retryCountKey, []string{idStr})
	q.reportEvent(AckEvent, 1)
	return nil
}

func (q *DelayQueue) updateZSetScore(key string, score float64, member string) error {
	scoreStr := strconv.FormatFloat(score, 'f', -1, 64)
	_, err := q.eval(updateZSetScoreScript, []string{key}, []interface{}{scoreStr, member})
	return err
}

func (q *DelayQueue) nack(idStr string) error {
	atomic.AddInt32(&q.fetchCount, -1)
	retryTime := float64(time.Now().Add(q.nackRedeliveryDelay).Unix())
	// if message consumption has not reach deadlin (still in unAckKey), then update its retry time
	err := q.updateZSetScore(q.unAckKey, retryTime, idStr)
	if err != nil {
		return fmt.Errorf("negative ack failed: %v", err)
	}
	q.reportEvent(NackEvent, 1)
	return nil
}

func (q *DelayQueue) unack2Retry() error {
	keys := []string{q.unAckKey, q.retryCountKey, q.retryKey, q.garbageKey}
	now := time.Now()
	raw, err := q.eval(unack2RetryScript, keys, []interface{}{now.Unix()})
	if err != nil && err != NilErr {
		return fmt.Errorf("unack to retry script failed: %v", err)
	}
	infos, ok := raw.([]interface{})
	if ok && len(infos) == 2 {
		retryCount, ok := infos[0].(int64)
		if ok {
			q.reportEvent(RetryEvent, int(retryCount))
		}
		failCount, ok := infos[1].(int64)
		if ok {
			q.reportEvent(FinalFailedEvent, int(failCount))
		}
	}
	return nil
}

func (q *DelayQueue) garbageCollect() error {
	msgIds, err := q.redisCli.SMembers(q.garbageKey)
	if err != nil {
		return fmt.Errorf("smembers failed: %v", err)
	}
	if len(msgIds) == 0 {
		return nil
	}
	// allow concurrent clean
	msgKeys := make([]string, 0, len(msgIds))
	for _, idStr := range msgIds {
		msgKeys = append(msgKeys, q.genMsgKey(idStr))
	}
	err = q.redisCli.Del(msgKeys)
	if err != nil && err != NilErr {
		return fmt.Errorf("del msgs failed: %v", err)
	}
	err = q.redisCli.SRem(q.garbageKey, msgIds)
	if err != nil && err != NilErr {
		return fmt.Errorf("remove from garbage key failed: %v", err)
	}
	return nil
}

func (q *DelayQueue) beforeConsume() ([]string, error) {
	// pending to ready
	err := q.pending2Ready()
	if err != nil {
		return nil, err
	}
	// ready2Unack
	// prioritize new message consumption to avoid avalanches
	ids := make([]string, 0, q.fetchLimit)
	var fetchCount int32
	for {
		fetchCount = atomic.LoadInt32(&q.fetchCount)
		if q.fetchLimit > 0 && fetchCount >= int32(q.fetchLimit) {
			break
		}
		idStr, err := q.ready2Unack()
		if err == NilErr { // consumed all
			break
		}
		if err != nil {
			return nil, err
		}
		ids = append(ids, idStr)
		atomic.AddInt32(&q.fetchCount, 1)
	}
	// retry2Unack
	if fetchCount < int32(q.fetchLimit) || q.fetchLimit == 0 {
		for {
			fetchCount = atomic.LoadInt32(&q.fetchCount)
			if q.fetchLimit > 0 && fetchCount >= int32(q.fetchLimit) {
				break
			}
			idStr, err := q.retry2Unack()
			if err == NilErr { // consumed all
				break
			}
			if err != nil {
				return nil, err
			}
			ids = append(ids, idStr)
			atomic.AddInt32(&q.fetchCount, 1)
		}
	}
	return ids, nil
}

func (q *DelayQueue) afterConsume() error {
	// unack to retry
	err := q.unack2Retry()
	if err != nil {
		return err
	}
	err = q.garbageCollect()
	if err != nil {
		return err
	}
	return nil
}

func (q *DelayQueue) setRunning() {
	atomic.StoreInt32(&q.running, 1)
}

func (q *DelayQueue) setNotRunning() {
	atomic.StoreInt32(&q.running, 0)
}

func (q *DelayQueue) goWithRecover(fn func()) {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				q.logger.Printf("panic: %v\n", err)
			}
		}()
		fn()
	}()
}

// StartConsume creates a goroutine to consume message from DelayQueue
// use `<-done` to wait consumer stopping
// If there is no callback set, StartConsume will panic
func (q *DelayQueue) StartConsume() (done <-chan struct{}) {
	if q.cb == nil {
		panic("this instance has no callback")
	}
	q.close = make(chan struct{}, 1)
	q.setRunning()
	q.ticker = time.NewTicker(q.fetchInterval)
	q.consumeBuffer = make(chan string, q.fetchLimit)
	done0 := make(chan struct{})
	// start worker
	for i := 0; i < int(q.concurrent); i++ {
		q.goWithRecover(func() {
			for id := range q.consumeBuffer {
				q.callback(id)
				q.afterConsume()
			}
		})
	}
	// start main loop
	go func() {
	tickerLoop:
		for {
			select {
			case <-q.ticker.C:
				ids, err := q.beforeConsume()
				if err != nil {
					q.logger.Printf("before consume error: %v", err)
				}
				q.goWithRecover(func() {
					for _, id := range ids {
						q.consumeBuffer <- id
					}
				})
				// Always do unack2Retry and garbageCollect even there is no new messages
				// https://github.com/HDT3213/delayqueue/issues/21
				err = q.afterConsume()
				if err != nil {
					q.logger.Printf("after consume error: %v", err)
				}
			case <-q.close:
				break tickerLoop
			}
		}
		close(done0)
	}()
	return done0
}

// StopConsume stops consumer goroutine
func (q *DelayQueue) StopConsume() {
	close(q.close)
	q.setNotRunning()
	if q.ticker != nil {
		q.ticker.Stop()
	}
}

// GetPendingCount returns the number of pending messages
func (q *DelayQueue) GetPendingCount() (int64, error) {
	return q.redisCli.ZCard(q.pendingKey)
}

// GetReadyCount returns the number of messages which have arrived delivery time but but have not been delivered
func (q *DelayQueue) GetReadyCount() (int64, error) {
	return q.redisCli.LLen(q.readyKey)
}

// GetProcessingCount returns the number of messages which are being processed
func (q *DelayQueue) GetProcessingCount() (int64, error) {
	return q.redisCli.ZCard(q.unAckKey)
}

// ListenEvent register a listener which will be called when events occur,
// so it can be used to monitor running status
//
// But It can ONLY receive events from the CURRENT INSTANCE,
// if you want to listen to all events in queue, just use Monitor.ListenEvent
//
// There can be AT MOST ONE EventListener in an DelayQueue instance.
// If you are using customized listener, Monitor will stop working
func (q *DelayQueue) ListenEvent(listener EventListener) {
	q.eventListener = listener
}

// RemoveListener stops reporting events to EventListener
func (q *DelayQueue) DisableListener() {
	q.eventListener = nil
}

func (q *DelayQueue) reportEvent(code int, count int) {
	listener := q.eventListener // eventListener may be changed during running
	if listener != nil && count > 0 {
		event := &Event{
			Code:      code,
			Timestamp: time.Now().Unix(),
			MsgCount:  count,
		}
		listener.OnEvent(event)
	}
}

// EnableReport enables reporting to monitor
func (q *DelayQueue) EnableReport() {
	reportChan := genReportChannel(q.name)
	q.ListenEvent(&pubsubListener{
		redisCli:   q.redisCli,
		reportChan: reportChan,
	})
}

// DisableReport stops reporting to monitor
func (q *DelayQueue) DisableReport() {
	q.DisableListener()
}

// MessageInfo stores information to trace a message
type MessageInfo struct {
	id string
}

func (msg *MessageInfo) ID() string {
	return msg.id
}

type InterceptResult struct {
	Intercepted bool
	State       string
}

func genReportChannel(name string) string {
	return "dq:" + name + ":reportEvents"
}
