package rate_limit

type Service interface {
	AddInstanceToCluster(instanceID string) error // add instance to cluster : use instance ID or instance IP
	GetInstanceRateLimit() (string, error)        // get instance for routing with rate limit
}
