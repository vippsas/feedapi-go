package feedmanager

import (
	"context"
	"errors"
	"feedapi-service/feeddatabase"
	"github.com/sirupsen/logrus"
	"github.com/vippsas/feedapi-go/feedapi"
)

var (
	UnsupportedPartition = errors.New("partition is not supported")
	TooManyPartitions    = errors.New("not able to initialize partitions, too many partitions")
	PartitionIsClosed    = errors.New("partition is closed")
	OutOfSync            = errors.New("out of sync")
)

type PartitionManager interface {
	FetchActivePartitions(context.Context) ([]ActivePartition, error)
	UpdateCursor(context.Context, logrus.FieldLogger, int, string) error
	ValidateFeed(context.Context, logrus.FieldLogger, feedapi.FeedInfo) error
}

type partitionManagerImpl struct {
	feedInfoDBManager feeddatabase.FeedPartitionSqlRepository
}

func NewPartitionManager(feedInfoDBManager feeddatabase.FeedPartitionSqlRepository) PartitionManager {
	return &partitionManagerImpl{
		feedInfoDBManager: feedInfoDBManager,
	}
}

type ActivePartition struct {
	PartitionID int
	Cursor      string
}

func (manager *partitionManagerImpl) FetchActivePartitions(ctx context.Context) ([]ActivePartition, error) {
	partitions, err := manager.feedInfoDBManager.FetchPartitions(ctx)
	if err != nil {
		return nil, err
	}

	var activePartitions []ActivePartition
	for _, partition := range partitions {
		if partition.Closed {
			continue
		}

		activePartition := ActivePartition{
			PartitionID: partition.PartitionID,
			Cursor:      partition.Cursor,
		}
		activePartitions = append(activePartitions, activePartition)
	}

	return activePartitions, nil
}

func (manager *partitionManagerImpl) UpdateCursor(ctx context.Context, logger logrus.FieldLogger, partition int, cursor string) error {
	return manager.feedInfoDBManager.UpdateCursor(ctx, logger, partition, cursor)
}

func (manager *partitionManagerImpl) ValidateFeed(ctx context.Context, logger logrus.FieldLogger, feedInfo feedapi.FeedInfo) error {
	logger.
		WithField("event", "info.user_feed_api.validate_partitions").
		Info("Starting to validate feed")

	storedPartitions, err := manager.feedInfoDBManager.FetchPartitions(ctx)
	if err != nil {
		return err
	}

	// Validate that there are no changes to the feed
	if len(feedInfo.Partitions) > len(storedPartitions) {
		// If there are more partitions in the feed than we support we need to fail since we need to do manual intervention to support the new partitions
		logger.
			WithField("event", "error.user_feed_api.invalid_partition_config").
			Errorf("There are more partitions in the feed than we support, number of partitions in feed %d, supported number of partitions %d", len(feedInfo.Partitions), len(storedPartitions))

		return TooManyPartitions
	} else if len(feedInfo.Partitions) == len(storedPartitions) {
		for _, partition := range feedInfo.Partitions {
			storedPartition, partitionFound := storedPartitions[partition.Id]
			if !partitionFound {
				// If the partition is not found in the stored partitions we need to fail since we need to do manual intervention to support the new partition
				logger.
					WithField("event", "error.user_feed_api.invalid_partition_config").
					Errorf("Partitions with ID: %d does not match any stored partitions", feedInfo.Partitions[0].Id)

				return UnsupportedPartition
			} else if partition.Closed && !storedPartition.Closed {
				// Give error is the partition in feed is closed while the stored partition is not
				// If the feed partition is closed we need to do manual action to close the stored partition such that we stop processing that partition
				logger.
					WithField("event", "error.user_feed_api.partition_closed").
					Errorf("Partition with ID: %d is closed", partition.Id)

				return PartitionIsClosed
			} else {
				// All good, log that we validated the partition
				logger.
					WithField("event", "info.user_feed_api.partition_validated").
					Infof("Validated partition with ID: %d", partition.Id)
			}
		}
	} else {
		// Fewer partitions in feed than we support
		logger.
			WithField("event", "error.user_feed_api.feed_info_out_of_sync").
			Error("Feed info is out of sync")

		return OutOfSync
	}

	logger.
		WithField("event", "info.user_feed_api.validate_partitions_finished").
		Info("Finished validating feed")

	return nil
}
