package basic_service

import (
	"context"
	"github.com/go-redis/redis/v8"
	"strconv"
	"time"
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

func (s *Service) PushMemberToDelayedQueue(queue string, member string, timeoutInSeconds int) error {
	timestamp := time.Now().Add(time.Duration(timeoutInSeconds) * time.Second).Unix()
	_, err := s.RedisClient.ZAdd(context.Background(), queue, &redis.Z{
		Score:  float64(timestamp),
		Member: member,
	}).Result()
	return err
}
func (s *Service) GetMemberTimeoutInDelayedQueue(queue string, scanSize int64) ([]string, error) {
	return s.RedisClient.ZRangeByScore(context.Background(), queue, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    strconv.FormatInt(time.Now().Unix(), 10),
		Offset: 0,
		Count:  scanSize,
	}).Result()
}

func (s *Service) AcquireLock(key, value string, timeout time.Duration) (bool, error) {
	return s.RedisClient.SetNX(context.Background(), key, value, timeout).Result()
}

func (s *Service) ReleaseLock(key, value string) error {
	luaScript := `
	if redis.call("get", KEYS[1]) == ARGV[1] then
		return redis.call("del", KEYS[1])
	else
		return 0
	end
	`
	_, err := s.RedisClient.Eval(context.Background(), luaScript, []string{key}, value).Result()
	return err
}
