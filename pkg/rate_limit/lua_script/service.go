package lua_script

import (
	"fmt"
	"go-redis/pkg/basic_service"
	"go-redis/pkg/rate_limit/manage_request_cnt"
)

type Service struct {
	BaseService               *basic_service.Service
	ClusterQueueName          string
	Limit                     int
	TimeWindowInSeconds       int // limit ${limit} requests per ${timeWindowInSeconds} seconds
	ManageRequestCountService *manage_request_cnt.Service
}

func (s *Service) AddInstanceToCluster(instanceID string) error {
	return s.BaseService.AddMemberToSortedSet(s.ClusterQueueName, instanceID, 0)
}

var luaScript = `
		local key = KEYS[1]
		local min = ARGV[1]
		local max = ARGV[2]
		local firstItem = redis.call('ZRANGEBYSCORE', key, min, max, 'LIMIT', 0, 1)
		if #firstItem > 0 then
			local newItemScore = redis.call('ZINCRBY', key, 1, firstItem[1])
			return {firstItem[1], newItemScore}
		else
			return nil
		end
	`

func (s *Service) GetInstanceRateLimit() (string, error) {
	cmd, err := s.BaseService.ExecuteLuaScript(luaScript, []string{s.ClusterQueueName}, "-inf", fmt.Sprintf("(%v", s.Limit))
	if err != nil || cmd == nil {
		return "", err
	} else {
		instanceIp := cmd.([]interface{})[0].(string)
		s.ManageRequestCountService.PushDecreaseRequestCountMessage(instanceIp)
		return instanceIp, err
	}
}
