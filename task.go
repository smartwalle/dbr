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
	defaultPrefix = "tm"
	defaultInfix  = "task:manager"
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
		key = defaultPrefix
	}
	m.prefix = key

	for _, opt := range opts {
		opt.Apply(m)
	}

	if m.redSync == nil {
		m.redSync = NewRedSync(pool)
	}

	m.prefix = fmt.Sprintf("%s:%s", m.prefix, defaultInfix)
	m.eventPrefix = fmt.Sprintf("%s:event:", m.prefix)
	m.mutexPrefix = fmt.Sprintf("%s:mutex:", m.prefix)
	m.consumePrefix = fmt.Sprintf("%s:consume:", m.prefix)

	m.streamKey = fmt.Sprintf("%s:stream", m.prefix)
	m.streamGroup = fmt.Sprintf("%s:group", m.prefix)
	m.streamConsumer = fmt.Sprintf("%s:consumer", m.prefix)
	m.taskPool = make(map[string]*Task)

	m.location = time.Local
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

	var now = time.Now().Unix()
	var mutexKey = this.buildMutexKey(task.name)
	var redMu = this.redSync.NewMutex(mutexKey, WithRetryCount(4))
	if err := redMu.Lock(); err != nil {
		return
	}

	// 59 秒以内同一个任务只能被处理一次
	var consumeKey = this.buildConsumeKey(task.name)
	var muSess = this.rPool.GetSession()
	if rResult := muSess.SET(consumeKey, now, "EX", 59, "NX"); rResult.MustString() == "OK" {
		go task.handler(task.name)
	}
	muSess.Close()

	redMu.Unlock()

	// 重新激活任务
	this.runTask(task)
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

	return this.runTask(task)
}

func (this *TaskManager) runTask(task *Task) error {
	var now = time.Now().In(this.location)
	var next = task.schedule.Next(now).In(this.location)

	var ttl = next.Unix() - now.Unix()

	var rSess = this.rPool.GetSession()
	defer rSess.Close()

	var key = this.buildEventKey(task.name)
	var rResult = rSess.SET(key, time.Now().Unix(), "EX", ttl, "NX")
	return rResult.Error
}

func (this *TaskManager) RemoveTask(taskName string) {
	if taskName == "" {
		return
	}

	var rSess = this.rPool.GetSession()
	rSess.Close()

	rSess.BeginTx()
	rSess.Send("DEL", this.buildEventKey(taskName))
	rSess.Send("DEL", this.buildConsumeKey(taskName))
	rSess.Commit()
}
