package manage_request_cnt

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/meogau/go-platform/worker"
	"go-redis/pkg/basic_service"
	"strings"
	"sync"
	"time"
)

type Service struct {
	RedisBaseService               *basic_service.Service
	DecreaseRequestCountQueueName  string
	ManagerRequestCountQueueName   string
	WorkerCapacity                 int
	WorkerMessageQueueSize         int
	ScanIntervalInSeconds          int
	RequestCountTimeInSeconds      int
	LockProcessTimeoutInSeconds    int
	ScanSize                       int64
	decreaseRequestCountWorkerPool *worker.Pool
	status                         bool
	wg                             sync.WaitGroup
}

var configSync sync.Once

func (s *Service) Run() {
	s.status = true
	configSync.Do(func() {
		s.decreaseRequestCountWorkerPool = worker.NewWorkerPool(s.WorkerCapacity, s.WorkerMessageQueueSize, s.processDecreaseMessage)
		s.decreaseRequestCountWorkerPool.Run()
		s.wg.Add(1)
		for s.status {
			messages, err := s.RedisBaseService.GetMemberTimeoutInDelayedQueue(s.DecreaseRequestCountQueueName, s.ScanSize)
			if err != nil {
				_ = fmt.Errorf("error get member timeout in delayed queue: %v", err)
			}
			for _, message := range messages {
				s.decreaseRequestCountWorkerPool.PushMessage(message)
			}
			time.Sleep(time.Duration(s.ScanIntervalInSeconds) * time.Second)
		}
		s.wg.Done()
	})
}

func (s *Service) processDecreaseMessage(message interface{}) error {
	messageArr := strings.Split(message.(string), "-")
	if lock, err := s.RedisBaseService.AcquireLock(message.(string), message.(string), time.Second*time.Duration(s.LockProcessTimeoutInSeconds)); err == nil && lock {
		defer s.RedisBaseService.ReleaseLock(message.(string), message.(string))
		instanceIp := messageArr[0]
		err := s.RedisBaseService.DeleteMemberInSortedSet(s.DecreaseRequestCountQueueName, message.(string))
		if err != nil {
			return fmt.Errorf("error delete member in sorted set: %v", err)
		}
		return s.RedisBaseService.IncreaseMemberInSortedSet(s.ManagerRequestCountQueueName, instanceIp, -1)
	}
	return nil
}

func (s *Service) PushDecreaseRequestCountMessage(instanceIp string) {
	err := s.RedisBaseService.PushMemberToDelayedQueue(s.DecreaseRequestCountQueueName, fmt.Sprintf("%v-%v",
		instanceIp, uuid.New()), s.RequestCountTimeInSeconds)
	if err != nil {
		_ = fmt.Errorf("error push decrease request count message: %v", err)
	}
}

func (s *Service) Shutdown() {
	s.status = false
	s.wg.Wait()
	s.decreaseRequestCountWorkerPool.Shutdown()
}
