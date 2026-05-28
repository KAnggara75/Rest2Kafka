package service

import (
	"context"

	"github.com/KAnggara75/Rest2Kafka/internal/kafka"
	"github.com/KAnggara75/Rest2Kafka/internal/model"
)

type PublishService interface {
	Publish(ctx context.Context, clusterName, topic, key, value string) error
	ListClusters() []model.ClusterDetail
	ListTopics(ctx context.Context, clusterName string) ([]string, error)
}

type service struct {
	kafkaManager *kafka.Manager
}

func NewPublishService(kafkaManager *kafka.Manager) PublishService {
	return &service{
		kafkaManager: kafkaManager,
	}
}

func (s *service) Publish(ctx context.Context, clusterName, topic, key, value string) error {
	return s.kafkaManager.Publish(ctx, clusterName, topic, key, value)
}

func (s *service) ListClusters() []model.ClusterDetail {
	return s.kafkaManager.ListClusters()
}

func (s *service) ListTopics(ctx context.Context, clusterName string) ([]string, error) {
	return s.kafkaManager.ListTopics(ctx, clusterName)
}
