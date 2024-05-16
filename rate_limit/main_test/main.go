package main

import (
	"fmt"
	"go-redis/basic_service"
	"go-redis/client"
	"go-redis/rate_limit/lua_script"
	"go-redis/rate_limit/redis_transaction"
	"net/http"
)

func main() {
	baseService := &basic_service.Service{
		RedisClient: client.GetSimpleRedisClient("localhost:6379", "", "", 0),
	}
	//init lua cluster
	limitLuaService = &lua_script.Service{
		BaseService:         baseService,
		ClusterQueueName:    "cluster_queue_lua",
		Limit:               10,
		TimeWindowInSeconds: 5,
		//limit 10 request in 5 seconds
	}
	_ = limitLuaService.AddInstanceToCluster("instance1")
	_ = limitLuaService.AddInstanceToCluster("instance2")

	//init transaction cluster
	limitTransactionService = &redis_transaction.Service{
		BaseService:         baseService,
		ClusterQueueName:    "cluster_queue_transaction",
		Limit:               10,
		TimeWindowInSeconds: 5,
	}
	_ = limitTransactionService.AddInstanceToCluster("instance1")
	_ = limitTransactionService.AddInstanceToCluster("instance2")

	//init api test
	http.HandleFunc("/limit/lua", rateLimitedHandlerWithLua)
	http.HandleFunc("/limit/transaction", rateLimitedHandlerWithTransaction)
	err := http.ListenAndServe(":1402", nil)
	if err != nil {
		return
	}
}

var (
	limitLuaService         *lua_script.Service
	limitTransactionService *redis_transaction.Service
)

func rateLimitedHandlerWithLua(w http.ResponseWriter, _ *http.Request) {
	instance, err := limitLuaService.GetInstanceRateLimit()
	if err != nil || len(instance) == 0 {
		_, _ = fmt.Fprintf(w, "limited request")
		return
	} else {
		_, _ = fmt.Fprintf(w, instance)
	}

}
func rateLimitedHandlerWithTransaction(w http.ResponseWriter, _ *http.Request) {
	instance, err := limitTransactionService.GetInstanceRateLimit()
	if err != nil || len(instance) == 0 {
		_, _ = fmt.Fprintf(w, "limited request")
		return
	} else {
		_, _ = fmt.Fprintf(w, instance)
	}
}
