package dbr

import (
	"errors"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"strings"
	"sync"
	"time"
)

const (
	defaultKey   = "default"
	defaultInfix = "task:manager"
)

var (
	ErrTaskExists = errors.New("dbr: task already exists")
)

type TaskHandler func(name string)

type TaskOption interface {
	Apply(*TaskManager)
}

type taskOptionFunc func(*TaskManager)

func (f taskOptionFunc) Apply(tm *TaskManager) {
	f(tm)
}

func WithTaskRedSync(redSync *RedSync) TaskOption {
	return taskOptionFunc(func(tm *TaskManager) {
		tm.redSync = redSync
	})
}

func WithTaskLocation(location *time.Location) TaskOption {
	return taskOptionFunc(func(tm *TaskManager) {
		tm.location = location
	})
}

type TaskManager struct {
	prefix        string
	eventPrefix   string
	mutexPrefix   string
	consumePrefix string

	rPool   Pool
	redSync *RedSync

	streamKey      string
	streamGroup    string
	streamConsumer string

	mu       sync.RWMutex
	taskPool map[string]*Task

	location *time.Location
	parser   Parser
}

type Task struct {
	name     string
	schedule Schedule
	handler  TaskHandler
}

func NewTaskManager(key string, pool Pool, opts ...TaskOption) *TaskManager {
	var m = &TaskManager{}
	m.rPool = pool

	key = strings.TrimSpace(key)
	if key == "" {
		key = defaultKey
	}
	m.prefix = key
	m.location = time.Local

	for _, opt := range opts {
		opt.Apply(m)
	}

	if m.redSync == nil {
		m.redSync = NewRedSync(pool)
	}

	m.prefix = fmt.Sprintf("%s:%s", defaultInfix, m.prefix)
	m.eventPrefix = fmt.Sprintf("%s:event:", m.prefix)
	m.mutexPrefix = fmt.Sprintf("%s:mutex:", m.prefix)
	m.consumePrefix = fmt.Sprintf("%s:consume:", m.prefix)

	m.streamKey = fmt.Sprintf("%s:stream", m.prefix)
	m.streamGroup = fmt.Sprintf("%s:group", m.prefix)
	m.streamConsumer = fmt.Sprintf("%s:consumer", m.prefix)
	m.taskPool = make(map[string]*Task)

	m.parser = NewParser(Minute | Hour | Dom | Month | Dow | Descriptor)

	go m.subscribe()
	go m.consumerTask(m.streamKey, m.streamGroup, m.streamConsumer)
	return m
}

func (this *TaskManager) buildEventKey(name string) string {
	return fmt.Sprintf("%s%s", this.eventPrefix, name)
}

func (this *TaskManager) buildMutexKey(name string) string {
	return fmt.Sprintf("%s%s", this.mutexPrefix, name)
}

func (this *TaskManager) buildConsumeKey(name string) string {
	return fmt.Sprintf("%s%s", this.consumePrefix, name)
}

func (this *TaskManager) subscribe() {
	var key = fmt.Sprintf("__keyspace@*__:%s", this.buildEventKey("*"))

	var rSess = this.rPool.GetSession()
	defer rSess.Close()

	var pConn = &redis.PubSubConn{Conn: rSess.Conn()}
	pConn.PSubscribe(key)

	for {
		switch data := pConn.Receive().(type) {
		case error:
		case redis.Message:
			var channels = strings.Split(data.Channel, this.eventPrefix)
			if len(channels) < 2 {
				continue
			}
			var taskName = channels[1]
			if taskName == "" {
				continue
			}

			var action = string(data.Data)
			if action == "expired" {
				this.postTask(taskName)
			}
		}
	}
}

func (this *TaskManager) postTask(taskName string) {
	var rSess = this.rPool.GetSession()
	defer rSess.Close()
	rSess.XADD(this.streamKey, 0, "*", "task_name", taskName)
}

func (this *TaskManager) consumerTask(key, group, consumer string) {
	var rSess = this.rPool.GetSession()
	defer rSess.Close()

	rSess.XGROUPCREATE(key, group, "$", "MKSTREAM")

	for {
		var streams, err = rSess.XREADGROUP(group, consumer, 1, 0, key, ">").Streams()
		if err != nil {
			continue
		}

		for _, stream := range streams {
			rSess.XACK(key, group, stream.Id)
			rSess.XDEL(key, stream.Id)

			var taskName = stream.Fields["task_name"]
			if taskName == "" {
				continue
			}

			go this.handleTask(taskName)
		}
	}
}

func (this *TaskManager) handleTask(taskName string) {
	this.mu.RLock()
	var task = this.taskPool[taskName]
	this.mu.RUnlock()

	if task == nil || task.handler == nil {
		return
	}

	var mutexKey = this.buildMutexKey(task.name)
	var redMu = this.redSync.NewMutex(mutexKey, WithRetryCount(4))
	if err := redMu.Lock(); err != nil {
		return
	}

	// 重新激活任务
	var next, _ = this.runTask(task)
	var expiresIn int64 = 59000
	if next-expiresIn < 1000 {
		expiresIn = next - 1000
	}

	// 同一个任务在一分钟（最大时间）内只能被处理一次
	var consumeKey = this.buildConsumeKey(task.name)
	var rSess = this.rPool.GetSession()
	if rResult := rSess.SET(consumeKey, time.Now(), "PX", expiresIn, "NX"); rResult.MustString() == "OK" {
		go task.handler(task.name)
	}
	rSess.Close()

	redMu.Unlock()
}

func (this *TaskManager) AddTask(name, spec string, handler TaskHandler) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	schedule, err := this.parser.Parse(spec)
	if err != nil {
		return err
	}

	this.mu.RLock()
	if _, ok := this.taskPool[name]; ok {
		this.mu.RUnlock()
		return ErrTaskExists
	}
	this.mu.RUnlock()

	var task = &Task{}
	task.name = name
	task.schedule = schedule
	task.handler = handler

	this.mu.Lock()
	this.taskPool[name] = task
	this.mu.Unlock()

	_, err = this.runTask(task)
	return err
}

func (this *TaskManager) runTask(task *Task) (next int64, err error) {
	var rSess = this.rPool.GetSession()
	var key = this.buildEventKey(task.name)

	var now = time.Now().In(this.location)
	var nextTime = task.schedule.Next(now).In(this.location)
	next = (nextTime.UnixNano() - now.UnixNano()) / 1e6

	var rResult = rSess.SET(key, nextTime, "PX", next, "NX")
	rSess.Close()

	if rResult.Error != nil {
		return -1, rResult.Error
	}
	return next, nil
}

func (this *TaskManager) RemoveTask(taskName string) {
	if taskName == "" {
		return
	}

	this.mu.Lock()
	delete(this.taskPool, taskName)
	this.mu.Unlock()

	var rSess = this.rPool.GetSession()
	defer rSess.Close()

	rSess.BeginTx()
	rSess.Send("DEL", this.buildEventKey(taskName))
	rSess.Send("DEL", this.buildConsumeKey(taskName))
	rSess.Commit()
}
