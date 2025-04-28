//go:build test

package feedapi_service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/franela/goblin"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlserver"
	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vippsas/go-leaderelection"
	"github.com/vippsas/verified-email/app/feature_toggle"
	"github.com/vippsas/verified-email/app/feed_api_manager/feeddatabase"
	"net/http"
	"net/url"
	"testing"
	"time"
)

const (
	initialCursor = "_first"
	feedApiHost   = "http://localhost:8390"
	serviceUrl    = feedApiHost + "/api/v1/feed"

	feedPullInterval    = 50 * time.Millisecond
	feedBackOffInterval = 50 * time.Millisecond
	pageSize            = 1
	sqlTimeout          = 50 * time.Millisecond
)

type GenericEventProcessorMock struct {
	mock.Mock
}

func (process *GenericEventProcessorMock) Process(ctx context.Context, data json.RawMessage) {
	process.Called(ctx, data)
}

// NB: This test requires docker images to be running
// This test is only meant to test the library and not domain specific logic
func TestProcess(t *testing.T) {
	g := goblin.Goblin(t)

	g.Describe("Test feed api", func() {
		var err error
		var feedInfoManager feeddatabase.FeedPartitionSqlRepository

		testLogger := logrus.New()
		requestProcessor := func(r *http.Request) error {
			return nil
		}

		DB, err := initDatabaseAndRunMigration("../../library/db/migrations")
		if err != nil {
			fmt.Println("Error: " + err.Error())
			t.Fail()
			return
		}
		feedInfoManager, _ = feeddatabase.Init(DB, sqlTimeout)

		g.BeforeEach(func() {
			resetAndSetDefaultFeedResponse(testLogger)
			resetFeedApiDb(DB)
		})

		g.After(func() {
			resetFeedApiDb(DB)
		})

		g.It("Validate that event was processed and cursor updated", func() {
			finalCtx, finalCancel, processCtx, processCancel := setupContext(testLogger)
			defer processCancel()

			eventProcessor := GenericEventProcessorMock{}
			eventProcessor.On("Process", mock.Anything, mock.Anything)

			unleashMock := feature_toggle.CreateMockUnleashClient()

			process, err := CreateFeedApiProcess(finalCtx, feedInfoManager, &eventProcessor, unleashMock, serviceUrl, feedPullInterval, feedBackOffInterval, pageSize, requestProcessor)
			assert.Nil(g, err)
			go process.ConsumeFeedApi(processCtx)
			time.Sleep(150 * time.Millisecond)
			finalCancel()

			eventProcessor.AssertCalled(t, "Process", mock.Anything, mock.Anything)
			partitions, err := fetchPartitions(context.Background(), DB)
			assert.Nil(g, err)
			assert.Equal(g, 1, len(partitions))
			assert.Equal(g, "1", partitions[0].Cursor)
		})

		g.It("Do not update cursor of process events if cursor is the same", func() {
			finalCtx, finalCancel, processCtx, processCancel := setupContext(testLogger)
			defer processCancel()

			setFeedApiResponse(testLogger, initialCursor, initialCursor)

			eventProcessor := GenericEventProcessorMock{}
			eventProcessor.On("Process", mock.Anything, mock.Anything)

			unleashMock := feature_toggle.CreateMockUnleashClient()

			process, err := CreateFeedApiProcess(finalCtx, feedInfoManager, &eventProcessor, unleashMock, serviceUrl, feedPullInterval, feedBackOffInterval, pageSize, requestProcessor)
			assert.Nil(g, err)
			go process.ConsumeFeedApi(processCtx)
			time.Sleep(150 * time.Millisecond)
			finalCancel()

			eventProcessor.AssertNotCalled(t, "Process", mock.Anything, mock.Anything)
			partitions, err := fetchPartitions(context.Background(), DB)
			assert.Nil(g, err)
			assert.Equal(g, 1, len(partitions))
			assert.Equal(g, initialCursor, partitions[0].Cursor)
		})

		g.It("Empty cursor is not supported", func() {
			finalCtx, finalCancel, processCtx, processCancel := setupContext(testLogger)
			defer processCancel()

			setFeedApiResponse(testLogger, initialCursor, "")

			eventProcessor := GenericEventProcessorMock{}
			eventProcessor.On("Process", mock.Anything, mock.Anything)

			unleashMock := feature_toggle.CreateMockUnleashClient()

			process, err := CreateFeedApiProcess(finalCtx, feedInfoManager, &eventProcessor, unleashMock, serviceUrl, feedPullInterval, feedBackOffInterval, pageSize, requestProcessor)
			assert.Nil(g, err)
			go process.ConsumeFeedApi(processCtx)
			time.Sleep(150 * time.Millisecond)
			finalCancel()

			eventProcessor.AssertNotCalled(t, "Process", mock.Anything, mock.Anything)
			partitions, err := fetchPartitions(context.Background(), DB)
			assert.Nil(g, err)
			assert.Equal(g, 1, len(partitions))
			assert.Equal(g, initialCursor, partitions[0].Cursor)
		})

		g.It("Recover from panic and check cursor is not updated", func() {
			finalCtx, finalCancel, processCtx, processCancel := setupContext(testLogger)
			defer processCancel()

			eventProcessor := GenericEventProcessorMock{}
			eventProcessor.On("Process", mock.Anything, mock.Anything).Panic("panic")

			unleashMock := feature_toggle.CreateMockUnleashClient()

			process, err := CreateFeedApiProcess(finalCtx, feedInfoManager, &eventProcessor, unleashMock, serviceUrl, feedPullInterval, feedBackOffInterval, pageSize, requestProcessor)
			assert.Nil(g, err)
			go process.ConsumeFeedApi(processCtx)
			time.Sleep(150 * time.Millisecond)
			finalCancel()

			eventProcessor.AssertCalled(t, "Process", mock.Anything, mock.Anything)
			partitions, err := fetchPartitions(context.Background(), DB)
			assert.Nil(g, err)
			assert.Equal(g, 1, len(partitions))
			assert.Equal(g, initialCursor, partitions[0].Cursor)
		})

		g.It("Feed response only containing same cursor should not be updated", func() {
			finalCtx, finalCancel, processCtx, processCancel := setupContext(testLogger)
			defer processCancel()

			setCursorResponse(testLogger, initialCursor, initialCursor)

			eventProcessor := GenericEventProcessorMock{}
			eventProcessor.On("Process", mock.Anything, mock.Anything)

			unleashMock := feature_toggle.CreateMockUnleashClient()

			process, err := CreateFeedApiProcess(finalCtx, feedInfoManager, &eventProcessor, unleashMock, serviceUrl, feedPullInterval, feedBackOffInterval, pageSize, requestProcessor)
			assert.Nil(g, err)
			go process.ConsumeFeedApi(processCtx)
			time.Sleep(150 * time.Millisecond)
			finalCancel()

			eventProcessor.AssertNotCalled(t, "Process", mock.Anything, mock.Anything)
			partitions, err := fetchPartitions(context.Background(), DB)
			assert.Nil(g, err)
			assert.Equal(g, 1, len(partitions))
			assert.Equal(g, initialCursor, partitions[0].Cursor)
		})

		g.It("Feed response only containing new cursor should be updated", func() {
			finalCtx, finalCancel, processCtx, processCancel := setupContext(testLogger)
			defer processCancel()

			finalCursor := "1"
			setCursorResponse(testLogger, initialCursor, finalCursor)

			eventProcessor := GenericEventProcessorMock{}
			eventProcessor.On("Process", mock.Anything, mock.Anything)

			unleashMock := feature_toggle.CreateMockUnleashClient()

			process, err := CreateFeedApiProcess(finalCtx, feedInfoManager, &eventProcessor, unleashMock, serviceUrl, feedPullInterval, feedBackOffInterval, pageSize, requestProcessor)
			assert.Nil(g, err)
			go process.ConsumeFeedApi(processCtx)
			time.Sleep(150 * time.Millisecond)
			finalCancel()

			eventProcessor.AssertNotCalled(t, "Process", mock.Anything, mock.Anything)
			partitions, err := fetchPartitions(context.Background(), DB)
			assert.Nil(g, err)
			assert.Equal(g, 1, len(partitions))
			assert.Equal(g, finalCursor, partitions[0].Cursor)
		})

		g.It("Do not process feed when feature is not enabled", func() {
			finalCtx, finalCancel, processCtx, processCancel := setupContext(testLogger)
			defer processCancel()

			eventProcessor := GenericEventProcessorMock{}
			eventProcessor.On("Process", mock.Anything, mock.Anything)

			unleashMock := feature_toggle.CreateMockUnleashClient()
			unleashMock.Disable(feature_toggle.EnableFeedApi)

			process, err := CreateFeedApiProcess(finalCtx, feedInfoManager, &eventProcessor, unleashMock, serviceUrl, feedPullInterval, feedBackOffInterval, pageSize, requestProcessor)
			assert.Nil(g, err)
			go process.ConsumeFeedApi(processCtx)
			time.Sleep(150 * time.Millisecond)
			finalCancel()

			eventProcessor.AssertNotCalled(t, "Process", mock.Anything, mock.Anything)
			partitions, err := fetchPartitions(context.Background(), DB)
			assert.Nil(g, err)
			assert.Equal(g, 1, len(partitions))
			assert.Equal(g, "_first", partitions[0].Cursor)
		})

		g.It("Calling process cancel should stop process", func() {
			finalCtx, finalCancel, processCtx, processCancel := setupContext(testLogger)
			defer finalCancel()

			eventProcessor := GenericEventProcessorMock{}
			eventProcessor.On("Process", mock.Anything, mock.Anything)

			unleashMock := feature_toggle.CreateMockUnleashClient()

			process, err := CreateFeedApiProcess(finalCtx, feedInfoManager, &eventProcessor, unleashMock, serviceUrl, feedPullInterval, feedBackOffInterval, pageSize, requestProcessor)
			assert.Nil(g, err)
			go process.ConsumeFeedApi(processCtx)
			time.Sleep(150 * time.Millisecond)
			processCancel()

			eventProcessor.AssertCalled(t, "Process", mock.Anything, mock.Anything)
			partitions, err := fetchPartitions(context.Background(), DB)
			assert.Nil(g, err)
			assert.Equal(g, 1, len(partitions))
			assert.Equal(g, "1", partitions[0].Cursor)
		})
	})
}

func initDatabaseAndRunMigration(migrationPath string) (*sql.DB, error) {
	urlInfo := url.Values{}
	urlInfo.Add("database", "verified-email-db")
	urlInfo.Add("applicationIntent", "ReadWrite")
	urlInfo.Add("log", "1")
	urlInfo.Add("encrypt", "disable")

	dbUrl := (&url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword("sa", "SuperSecret1337"),
		Host:     "localhost:1433",
		RawQuery: urlInfo.Encode(),
	}).String()

	db, err := sql.Open("sqlserver", dbUrl)
	if err != nil {
		return nil, err
	}

	driver, err := sqlserver.WithInstance(db, &sqlserver.Config{})
	if err != nil {
		return nil, err
	}

	var migrationSrc source.Driver
	if migrationSrc, err = (&file.File{}).Open("file://" + migrationPath); err != nil {
		return nil, err
	}

	var migration *migrate.Migrate
	if migration, err = migrate.NewWithInstance("file", migrationSrc, "sqlserver", driver); err != nil {
		return nil, err
	}

	if err = migration.Up(); err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			return db, nil
		} else {
			return nil, err
		}
	}
	return db, nil
}

func fetchPartitions(ctx context.Context, DB *sql.DB) (map[int]feeddatabase.FeedPartition, error) {
	QFetchPartitions := `
		SELECT PARTITION_ID, PARTITION_CURSOR, PARTITION_CLOSED
                FROM FEED_API.TBL_FEED_PARTITION`
	stmt, err := DB.Prepare(QFetchPartitions)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return nil, err
	}

	return mapFeedInfo(rows)
}

func mapFeedInfo(from *sql.Rows) (map[int]feeddatabase.FeedPartition, error) {
	partitions := make(map[int]feeddatabase.FeedPartition)
	for from.Next() {
		var partition feeddatabase.FeedPartition
		if err := from.Scan(&partition.PartitionID, &partition.Cursor, &partition.Closed); err != nil {
			return nil, err
		}
		partitions[partition.PartitionID] = partition
	}
	return partitions, nil
}

func resetFeedApiDb(DB *sql.DB) {
	_, _ = DB.Exec("DELETE FROM FEED_API.TBL_FEED_PARTITION")
	_, _ = DB.Exec("INSERT INTO FEED_API.TBL_FEED_PARTITION (PARTITION_ID, PARTITION_CURSOR, PARTITION_CLOSED) VALUES (0, '_first', 0)")
}

func resetAndSetDefaultFeedResponse(testLogger logrus.FieldLogger) {
	userId := uuid.Must(uuid.NewRandom()).String()
	eventType := "MyEvent"
	finalCursor := "1"

	err := RemoveFeedInfo(feedApiHost)
	if err != nil {
		testLogger.WithError(err).Error("Could not clear feed api")
	}

	err = SetFeedApiDiscoveryResponse(feedApiHost)
	if err != nil {
		testLogger.WithError(err).Error("Could not set expectation for feed api")
	}

	err = SetFeedApiPartitionResponse(feedApiHost, initialCursor, finalCursor, eventType, userId, "PersonId", "customerId", "market", "msisdn", "email")
	if err != nil {
		testLogger.WithError(err).Error("Could not set expectation for feed api")
	}

	err = SetFeedApiCursorResponse(feedApiHost, finalCursor)
	if err != nil {
		testLogger.WithError(err).Error("Could not set expectation for feed api")
	}
}

func setFeedApiResponse(testLogger logrus.FieldLogger, startCursor, endCursor string) {
	err := RemoveFeedInfo(feedApiHost)
	if err != nil {
		testLogger.WithError(err).Error("Could not clear feed api")
	}

	err = SetFeedApiDiscoveryResponse(feedApiHost)
	if err != nil {
		testLogger.WithError(err).Error("Could not set expectation for feed api")
	}

	userId := uuid.Must(uuid.NewRandom()).String()
	eventType := "MyEvent"
	err = SetFeedApiPartitionResponse(feedApiHost, startCursor, endCursor, eventType, userId, "PersonId", "customerId", "market", "msisdn", "email")
	if err != nil {
		testLogger.WithError(err).Error("Could not set expectation for feed api")
	}
}

func setCursorResponse(testLogger logrus.FieldLogger, startCursor, endCursor string) {
	err := RemoveFeedInfo(feedApiHost)
	if err != nil {
		testLogger.WithError(err).Error("Could not clear feed api")
	}

	err = SetFeedApiDiscoveryResponse(feedApiHost)
	if err != nil {
		testLogger.WithError(err).Error("Could not set expectation for feed api")
	}

	err = SetStartAndEndCursorFeedApiResponse(feedApiHost, startCursor, endCursor)
	if err != nil {
		testLogger.WithError(err).Error("Could not set expectation for feed api")
	}
}

func setupContext(testLogger logrus.FieldLogger) (context.Context, context.CancelFunc, context.Context, context.CancelFunc) {
	finalCtx, finalCancel := context.WithCancel(context.Background())
	finalCtx = leaderelection.ContextWithLogger(finalCtx, testLogger)
	// `processCtx` will also get cancelled if the parent context is cancelled but `processCancel` will only cancel `processCtx` and not the parent context
	processCtx, processCancel := context.WithCancel(finalCtx)
	processCtx = leaderelection.ContextWithLogger(processCtx, testLogger)
	return finalCtx, finalCancel, processCtx, processCancel
}
