package redis_transaction

import (
	"context"
	"github.com/go-redis/redis/v8"
	"go-redis/basic_service"
)

type Service struct {
	BaseService         *basic_service.Service
	ClusterQueueName    string
	Limit               int
	TimeWindowInSeconds int // limit ${limit} requests per ${timeWindowInSeconds} seconds
}

func (s *Service) AddInstanceToCluster(instanceID string) error {
	return s.BaseService.AddMemberToSortedSet(s.ClusterQueueName, instanceID, 0)
}

func (s *Service) GetInstanceRateLimit() (string, error) {
	var instance string
	ctx := context.Background()
	err := s.BaseService.RedisClient.Watch(ctx, func(tx *redis.Tx) error {
		results, err := tx.ZRangeByScore(ctx, s.ClusterQueueName, &redis.ZRangeBy{
			Min:   "-inf",
			Max:   "(10",
			Count: 1,
		}).Result()
		if err != nil {
			return err
		}

		if len(results) == 0 {
			return nil
		}
		firstItem := results[0]

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.ZIncrBy(ctx, s.ClusterQueueName, 1.0, firstItem)
			return nil
		})
		instance = firstItem
		if err != nil {
			return err
		}
		return nil
	}, s.ClusterQueueName)
	return instance, err
}
