package basic_service

import (
	"context"
	"github.com/go-redis/redis/v8"
)

type Service struct {
	RedisClient *redis.Client
}

func (s *Service) IncreaseMemberInSortedSet(key string, member string, score float64) error {
	_, err := s.RedisClient.ZIncrBy(context.Background(), key, score, member).Result()
	return err
}
func (s *Service) AddMemberToSortedSet(key string, member string, score float64) error {
	_, err := s.RedisClient.ZAdd(context.Background(), key, &redis.Z{
		Score:  score,
		Member: member,
	}).Result()
	return err
}
func (s *Service) DeleteMemberInSortedSet(key string, member string) error {
	_, err := s.RedisClient.ZRem(context.Background(), key, member).Result()
	return err
}
func (s *Service) ExecuteLuaScript(script string, keys []string, args ...interface{}) (interface{}, error) {
	return s.RedisClient.Eval(s.RedisClient.Context(), script, keys, args).Result()
}
