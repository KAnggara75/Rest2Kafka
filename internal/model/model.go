package model

type ClusterDetail struct {
	Name    string   `json:"name"`
	Brokers []string `json:"brokers"`
}

type PublishRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type PublishResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

type HealthResponse struct {
	Status string `json:"status"`
}

type ListClustersResponse struct {
	Clusters []ClusterDetail `json:"clusters"`
}

type ListTopicsResponse struct {
	Topics []string `json:"topics"`
}
