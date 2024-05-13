package client

import "github.com/go-redis/redis/v8"

func GetSimpleRedisClient(address string, username string, password string, db int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     address,
		Username: username,
		Password: password,
		DB:       db,
	})
}
