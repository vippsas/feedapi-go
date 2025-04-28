package feeddatabase

import (
	"context"
	"github.com/sirupsen/logrus"
)

type FeedPartitionSqlRepository interface {
	UpdateCursor(context.Context, logrus.FieldLogger, int, string) error
	FetchPartitions(context.Context) (map[int]FeedPartition, error)
}
