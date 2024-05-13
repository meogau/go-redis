package main

import (
	"fmt"
	"go-redis/basic_service"
	"go-redis/client"
	"go-redis/rate_limit/lua_script"
	"net/http"
)

func main() {
	limitLuaService = &lua_script.Service{
		BaseService: &basic_service.Service{
			RedisClient: client.GetSimpleRedisClient("localhost:6379", "", "", 0),
		},
		ClusterQueueName:    "cluster_queue",
		Limit:               10,
		TimeWindowInSeconds: 5,
		//limit 10 request in 5 seconds
	}
	_ = limitLuaService.AddInstanceToCluster("instance1")
	_ = limitLuaService.AddInstanceToCluster("instance2")
	http.HandleFunc("/test", rateLimitedHandler)
	err := http.ListenAndServe(":1402", nil)
	if err != nil {
		return
	}
}

var (
	limitLuaService *lua_script.Service
)

func rateLimitedHandler(w http.ResponseWriter, _ *http.Request) {
	instance, err := limitLuaService.GetInstanceRateLimit()
	if err != nil || len(instance) == 0 {
		_, _ = fmt.Fprintf(w, "limited request")
		return
	} else {
		_, _ = fmt.Fprintf(w, instance)
	}

}
