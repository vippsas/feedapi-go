//go:build test

package feedapi_service

import (
	"context"
	"fmt"
	"github.com/franela/goblin"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vippsas/go-leaderelection"
	"github.com/vippsas/verified-email/app/feature_toggle"
	"net/http"
	"testing"
	"time"
)

// NB: This test requires docker images to be running
// This test is only meant to test the library and not domain specific logic
func TestWorker(t *testing.T) {
	g := goblin.Goblin(t)

	g.Describe("Test generic worker", func() {
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

		g.BeforeEach(func() {
			resetFeedApiDb(DB)
			resetAndSetDefaultFeedResponse(testLogger)
		})

		g.After(func() {
			resetFeedApiDb(DB)
		})

		g.It("Test that worker started and processes the defined events", func() {
			testCtx, cancel := context.WithCancel(context.Background())
			testCtx = leaderelection.ContextWithLogger(testCtx, testLogger)

			workerConfig := WorkerConfig{
				ProcessID:                 "test",
				ProcessEffort:             1,
				ProcessNamespace:          "team-login",
				WorkerID:                  leaderelection.GenerateWorkerID(),
				Version:                   "test",
				HeartbeatInterval:         50 * time.Millisecond,
				WorkersNeededBeforeActive: 1,
				ShutDownTimeout:           50 * time.Millisecond,
			}

			eventProcessor := GenericEventProcessorMock{}
			eventProcessor.On("Process", mock.Anything, mock.Anything)

			unleashMock := feature_toggle.CreateMockUnleashClient()

			endChannel := make(chan struct{})
			defer close(endChannel)
			CreateAndRegisterFeedApiProcess(testCtx, workerConfig, serviceUrl, feedPullInterval, feedBackOffInterval, pageSize, DB, &eventProcessor, unleashMock, requestProcessor, endChannel)
			time.Sleep(500 * time.Millisecond)
			cancel()

			<-endChannel
			time.Sleep(500 * time.Millisecond)

			eventProcessor.AssertCalled(t, "Process", mock.Anything, mock.Anything)
			partitions, err := fetchPartitions(context.Background(), DB)
			assert.Nil(g, err)
			assert.Equal(g, 1, len(partitions))
			assert.Equal(g, "1", partitions[0].Cursor)
		})

		g.It("Test that worker handles panic from processor", func() {
			testCtx, cancel := context.WithCancel(context.Background())
			testCtx = leaderelection.ContextWithLogger(testCtx, testLogger)

			workerConfig := WorkerConfig{
				ProcessID:                 "test",
				ProcessEffort:             1,
				ProcessNamespace:          "team-login",
				WorkerID:                  leaderelection.GenerateWorkerID(),
				Version:                   "test",
				HeartbeatInterval:         50 * time.Millisecond,
				WorkersNeededBeforeActive: 1,
				ShutDownTimeout:           50 * time.Millisecond,
			}

			eventProcessor := GenericEventProcessorMock{}
			eventProcessor.On("Process", mock.Anything, mock.Anything).Panic("Error")

			unleashMock := feature_toggle.CreateMockUnleashClient()

			endChannel := make(chan struct{})
			defer close(endChannel)
			CreateAndRegisterFeedApiProcess(testCtx, workerConfig, serviceUrl, feedPullInterval, feedBackOffInterval, pageSize, DB, &eventProcessor, unleashMock, requestProcessor, endChannel)
			time.Sleep(500 * time.Millisecond)
			cancel()

			<-endChannel
			time.Sleep(500 * time.Millisecond)

			eventProcessor.AssertCalled(t, "Process", mock.Anything, mock.Anything)
			partitions, err := fetchPartitions(context.Background(), DB)
			assert.Nil(g, err)
			assert.Equal(g, 1, len(partitions))
			assert.Equal(g, initialCursor, partitions[0].Cursor)
		})

		g.It("Do not process feed when feature is not enabled", func() {
			testCtx, cancel := context.WithCancel(context.Background())
			testCtx = leaderelection.ContextWithLogger(testCtx, testLogger)

			workerConfig := WorkerConfig{
				ProcessID:                 "test",
				ProcessEffort:             1,
				ProcessNamespace:          "team-login",
				WorkerID:                  leaderelection.GenerateWorkerID(),
				Version:                   "test",
				HeartbeatInterval:         50 * time.Millisecond,
				WorkersNeededBeforeActive: 1,
				ShutDownTimeout:           50 * time.Millisecond,
			}

			eventProcessor := GenericEventProcessorMock{}
			eventProcessor.On("Process", mock.Anything, mock.Anything)

			unleashMock := feature_toggle.CreateMockUnleashClient()
			unleashMock.Disable(feature_toggle.EnableFeedApi)

			endChannel := make(chan struct{})
			defer close(endChannel)
			CreateAndRegisterFeedApiProcess(testCtx, workerConfig, serviceUrl, feedPullInterval, feedBackOffInterval, pageSize, DB, &eventProcessor, unleashMock, requestProcessor, endChannel)
			time.Sleep(500 * time.Millisecond)
			cancel()

			<-endChannel
			time.Sleep(500 * time.Millisecond)

			eventProcessor.AssertNotCalled(t, "Process", mock.Anything, mock.Anything)
			partitions, err := fetchPartitions(context.Background(), DB)
			assert.Nil(g, err)
			assert.Equal(g, 1, len(partitions))
			assert.Equal(g, "_first", partitions[0].Cursor)
		})
	})
}
