//go:build test

package feedmanager

import (
	"context"
	"errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/vippsas/feedapi-go/feedapi"
	"github.com/vippsas/verified-email/app/feed_api_manager/feeddatabase"
	"testing"
)

var (
	initialCursor    = "_first"
	initialPartition = 0
)

func createOnePartitionSqlMock() *SqlManagerMock {
	return &SqlManagerMock{
		partitionMap: map[int]feeddatabase.FeedPartition{
			initialPartition: createNewFeedPartition(initialPartition, false),
		},
	}
}

func createSqlMockWithValues(partitionId int, closed bool) *SqlManagerMock {
	partition := createNewFeedPartition(partitionId, closed)

	return &SqlManagerMock{
		partitionMap: map[int]feeddatabase.FeedPartition{
			partitionId: partition,
		},
	}
}

func createNewFeedPartition(partitionId int, closed bool) feeddatabase.FeedPartition {
	return feeddatabase.FeedPartition{
		PartitionID: partitionId,
		Cursor:      initialCursor,
		Closed:      closed,
	}
}

func createOnePartitionFeedInfo() feedapi.FeedInfo {
	return createFeedInfo(initialPartition, false)
}

func createFeedInfo(partitionId int, closed bool) feedapi.FeedInfo {
	return feedapi.FeedInfo{
		Token: "1",
		Partitions: []feedapi.Partition{
			{
				Id:     partitionId,
				Closed: closed,
			},
		},
		ExactlyOnce: false,
	}
}

func TestPartitionManager_ValidateFeed_MatchFeedInfoWithDBEntry(t *testing.T) {
	feedInfo := createOnePartitionFeedInfo()
	sqlMock := createOnePartitionSqlMock()
	testPartitionManager := NewPartitionManager(sqlMock)
	err := testPartitionManager.ValidateFeed(context.Background(), logrus.New(), feedInfo)
	assert.Nil(t, err)

	feedPartitions, err := testPartitionManager.FetchActivePartitions(context.Background())
	assert.Nil(t, err)

	assert.True(t, len(feedPartitions) == 1)
	assert.Equal(t, feedInfo.Partitions[0].Id, feedPartitions[0].PartitionID)
	assert.Equal(t, initialCursor, feedPartitions[0].Cursor)
}

func TestPartitionManager_ValidateFeed_GiveErrorWhenFeedInfoHarNoPartitions(t *testing.T) {
	feedInfo := feedapi.FeedInfo{}
	sqlMock := createOnePartitionSqlMock()
	testPartitionManager := NewPartitionManager(sqlMock)

	err := testPartitionManager.ValidateFeed(context.Background(), logrus.New(), feedInfo)
	assert.NotNil(t, err)
	assert.True(t, errors.Is(err, OutOfSync))
}

func TestPartitionManager_ValidateFeed_GiveErrorWhenDBNotInitialized(t *testing.T) {
	feedInfo := createOnePartitionFeedInfo()
	sqlMock := &SqlManagerMock{
		partitionMap: make(map[int]feeddatabase.FeedPartition),
	}
	testPartitionManager := NewPartitionManager(sqlMock)

	err := testPartitionManager.ValidateFeed(context.Background(), logrus.New(), feedInfo)
	assert.NotNil(t, err)
	assert.True(t, errors.Is(err, TooManyPartitions))
}

func TestPartitionManager_ValidateFeed_GiveErrorWhenFeedInfoHasTooManyPartitions(t *testing.T) {
	feedInfo := createOnePartitionFeedInfo()
	feedInfo.Partitions = append(feedInfo.Partitions, feedapi.Partition{Id: 2})
	sqlMock := createOnePartitionSqlMock()
	testPartitionManager := NewPartitionManager(sqlMock)

	err := testPartitionManager.ValidateFeed(context.Background(), logrus.New(), feedInfo)
	assert.NotNil(t, err)
	assert.True(t, errors.Is(err, TooManyPartitions))
}

func TestPartitionManager_ValidateFeed_GiveErrorWhenFeedInfoPartitionHasDifferentId(t *testing.T) {
	feedInfo := createFeedInfo(1, false)
	sqlMock := createOnePartitionSqlMock()
	testPartitionManager := NewPartitionManager(sqlMock)

	err := testPartitionManager.ValidateFeed(context.Background(), logrus.New(), feedInfo)
	assert.NotNil(t, err)
	assert.True(t, errors.Is(err, UnsupportedPartition))
}

func TestPartitionManager_ValidateFeed_GiveErrorWhenFeedInfoPartitionIsClosed(t *testing.T) {
	feedInfo := createFeedInfo(initialPartition, true)
	sqlMock := createOnePartitionSqlMock()
	testPartitionManager := NewPartitionManager(sqlMock)

	err := testPartitionManager.ValidateFeed(context.Background(), logrus.New(), feedInfo)
	assert.NotNil(t, err)
	assert.True(t, errors.Is(err, PartitionIsClosed))
}

func TestPartitionManager_ValidateFeed_ClosedPartitionInFeedInfoAndDBIsAllowed(t *testing.T) {
	feedInfo := createFeedInfo(initialPartition, true)
	sqlMock := createSqlMockWithValues(initialPartition, true)
	testPartitionManager := NewPartitionManager(sqlMock)

	err := testPartitionManager.ValidateFeed(context.Background(), logrus.New(), feedInfo)
	assert.Nil(t, err)
}

func TestPartitionManager_ValidateFeed_OnlyClosedPartitionInDBIsAllowed(t *testing.T) {
	feedInfo := createFeedInfo(initialPartition, false)
	sqlMock := createSqlMockWithValues(initialPartition, true)
	testPartitionManager := NewPartitionManager(sqlMock)

	err := testPartitionManager.ValidateFeed(context.Background(), logrus.New(), feedInfo)
	assert.Nil(t, err)
}

func TestPartitionManager_FetchActivePartitions_GetActivePartition(t *testing.T) {
	sqlMock := createOnePartitionSqlMock()
	testPartitionManager := NewPartitionManager(sqlMock)

	feedPartitions, err := testPartitionManager.FetchActivePartitions(context.Background())
	assert.Nil(t, err)

	assert.True(t, len(feedPartitions) == 1)
	assert.Equal(t, initialPartition, feedPartitions[0].PartitionID)
	assert.Equal(t, initialCursor, feedPartitions[0].Cursor)
}

func TestPartitionManager_FetchActivePartitions_OnlyInactivePartitionsReturnsEmptyList(t *testing.T) {
	sqlMock := createSqlMockWithValues(initialPartition, true)
	testPartitionManager := NewPartitionManager(sqlMock)

	feedPartitions, err := testPartitionManager.FetchActivePartitions(context.Background())
	assert.Nil(t, err)

	assert.True(t, len(feedPartitions) == 0)
}

func TestPartitionManager_FetchActivePartitions_OnlyGetActivePartitionWhenOnePartitionIsClosed(t *testing.T) {
	sqlMock := createSqlMockWithValues(initialPartition, false)
	closedPartitionId := 2
	closedPartition := createNewFeedPartition(closedPartitionId, true)
	sqlMock.partitionMap[closedPartitionId] = closedPartition
	testPartitionManager := NewPartitionManager(sqlMock)

	feedPartitions, err := testPartitionManager.FetchActivePartitions(context.Background())
	assert.Nil(t, err)

	assert.True(t, len(sqlMock.partitionMap) == 2)
	assert.True(t, sqlMock.partitionMap[closedPartitionId].Closed)

	assert.True(t, len(feedPartitions) == 1)
	assert.Equal(t, initialPartition, feedPartitions[0].PartitionID)
	assert.Equal(t, initialCursor, feedPartitions[0].Cursor)
}

func TestPartitionManager_UpdateCursor(t *testing.T) {
	newCursor := "newCursor"

	sqlMock := createOnePartitionSqlMock()
	testPartitionManager := NewPartitionManager(sqlMock)

	feedPartitions, err := testPartitionManager.FetchActivePartitions(context.Background())
	assert.Nil(t, err)
	assert.True(t, len(feedPartitions) == 1)
	assert.Equal(t, initialPartition, feedPartitions[0].PartitionID)
	assert.Equal(t, initialCursor, feedPartitions[0].Cursor)

	err = testPartitionManager.UpdateCursor(context.Background(), logrus.New(), initialPartition, newCursor)
	assert.Nil(t, err)

	feedPartitions, err = testPartitionManager.FetchActivePartitions(context.Background())
	assert.Nil(t, err)
	assert.True(t, len(feedPartitions) == 1)
	assert.Equal(t, initialPartition, feedPartitions[0].PartitionID)
	assert.Equal(t, newCursor, feedPartitions[0].Cursor)
}

func TestPartitionManager_UpdateCursor_UpdateClosedPartition(t *testing.T) {
	newCursor := "newCursor"

	sqlMock := createSqlMockWithValues(initialPartition, true)
	testPartitionManager := NewPartitionManager(sqlMock)

	feedPartitions, err := testPartitionManager.FetchActivePartitions(context.Background())
	assert.Nil(t, err)
	assert.True(t, len(feedPartitions) == 0)
	assert.True(t, len(sqlMock.partitionMap) == 1)
	assert.Equal(t, initialPartition, sqlMock.partitionMap[initialPartition].PartitionID)
	assert.Equal(t, initialCursor, sqlMock.partitionMap[initialPartition].Cursor)
	assert.True(t, sqlMock.partitionMap[initialPartition].Closed)

	err = testPartitionManager.UpdateCursor(context.Background(), logrus.New(), initialPartition, newCursor)
	assert.Nil(t, err)

	feedPartitions, err = testPartitionManager.FetchActivePartitions(context.Background())
	assert.Nil(t, err)

	assert.True(t, len(feedPartitions) == 0)
	assert.True(t, len(sqlMock.partitionMap) == 1)
	assert.Equal(t, initialPartition, sqlMock.partitionMap[initialPartition].PartitionID)
	assert.Equal(t, newCursor, sqlMock.partitionMap[initialPartition].Cursor)
	assert.True(t, sqlMock.partitionMap[initialPartition].Closed)
}

type SqlManagerMock struct {
	partitionMap map[int]feeddatabase.FeedPartition
}

func (mock SqlManagerMock) FetchPartitions(_ context.Context) (map[int]feeddatabase.FeedPartition, error) {
	partitions := make(map[int]feeddatabase.FeedPartition)
	for _, partition := range mock.partitionMap {
		partitions[partition.PartitionID] = partition
	}
	return partitions, nil
}

func (mock SqlManagerMock) UpdateCursor(_ context.Context, _ logrus.FieldLogger, partitionId int, cursor string) error {
	if item, ok := mock.partitionMap[partitionId]; ok {
		item.Cursor = cursor
		mock.partitionMap[partitionId] = item
	} else {
		return errors.New("partition not found")
	}
	return nil
}
