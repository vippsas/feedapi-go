package feedapi_service

import (
	"context"
	"database/sql"
	"feedapi-service/eventprocessor"
	"feedapi-service/feeddatabase"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/vippsas/go-leaderelection"
	"github.com/vippsas/go-leaderelection/mssqlrepository"
	"github.com/vippsas/go-leaderelection/repository"
	"github.com/vippsas/verified-email/app/feature_toggle"
	"net/http"
	"time"
)

func CreateAndRegisterFeedApiProcess(
	ctx context.Context,
	workerConfig WorkerConfig,
	serviceUrl string,
	feedPullInterval time.Duration,
	feedBackOffInterval time.Duration,
	pageSize int,
	DB *sql.DB,
	eventProcessor eventprocessor.EventProcessor,
	unleash feature_toggle.UnleashClient,
	requestProcessor func(r *http.Request) error,
	workerDoneChan chan struct{}) {

	logger, _ := leaderelection.LoggerFromContext(ctx)
	workerLogger := logger.WithFields(logrus.Fields{
		"Handler": "FeedApiWorker",
	})

	workerLogger.
		WithField("event", "info.feed_api_worker.initialize").
		WithField("version", workerConfig.Version).
		Infof("Initializing feed api worker")

	dbManager, _ := feeddatabase.Init(DB, 5*time.Second)
	feedApiProcess, err := CreateFeedApiProcess(ctx, dbManager, eventProcessor, unleash, serviceUrl, feedPullInterval, feedBackOffInterval, pageSize, requestProcessor)
	if err != nil {
		workerLogger.
			WithError(err).
			WithField("event", "error.init.user_service_feed_api_consumer").
			Error("Failed to set up user service feed api consumer")

		// Signal that the worker is done by sending an empty struct on the channel
		workerDoneChan <- struct{}{}
		return
	}

	worker := leaderelection.Worker{
		Namespace:                 workerConfig.ProcessNamespace,
		WorkerID:                  workerConfig.WorkerID,
		Version:                   workerConfig.Version,
		HeartbeatInterval:         workerConfig.HeartbeatInterval,
		WorkersNeededBeforeActive: workerConfig.WorkersNeededBeforeActive,
	}
	worker.RegisterProcess(workerConfig.ProcessID, workerConfig.ProcessEffort, workerConfig.TimeoutPadding, feedApiProcess.ConsumeFeedApi)

	repo := mssqlrepository.SQLRepository{
		DB:     DB,
		Logger: workerLogger,
	}

	go func() {
		SignalHandlingWorkerLoop(ctx, worker, workerLogger, repo, workerConfig.ShutDownTimeout)
		// Signal sent when the worker loop has finished and clean up is done
		workerDoneChan <- struct{}{}
	}()

	workerLogger.
		WithField("event", "info.feed_api_worker.finished").
		WithField("version", workerConfig.Version).
		Infof("Finished setting up feed api worker")
}

// SignalHandlingWorkerLoop
// Copy of SignalHandlingWorkerLoop in leaderelection/signalhandler.go
// Handle soft context cancel from the outside for graceful shutdown
// Let this function run until shut down has been completed
// ToDo: Move to library after testing
func SignalHandlingWorkerLoop(softCtx context.Context, worker leaderelection.Worker, logger logrus.FieldLogger, repo repository.Repository, timeout time.Duration) {
	// The worker uses 2 contexts: One for soft-closing (stop picking up work) and one for shutting down
	hardCtx, hardCancel := context.WithCancel(context.Background())
	defer hardCancel()

	shutdownChan := make(chan struct{}, 1)
	defer close(shutdownChan)

	// Ready! Launch the worker
	go func() {
		defer func() {
			if rc := recover(); rc != nil {
				routingLogger := logger.
					WithField("event", "error.feed_api_worker.panic_detected").
					WithField("PanicReason", rc)
				switch rc.(type) {
				case error:
					routingLogger = routingLogger.WithError(errors.WithStack(rc.(error)))
				default:
					// Do nothing
				}
				routingLogger.Error("Detected panic for feed api worker!")
				shutdownChan <- struct{}{}
			}
		}()

		// NOTE: Number of workers needed before active is set pr environment in the kustomization.yaml file, property: NEEDED_ACTIVE_WORKERS
		// The default is defined in UserServiceFeedApiConfig under config.go
		worker.MainLoop(softCtx, hardCtx, logger, repo)
		//After the worker.MainLoop returns signal on the shutdownChan
		//struct{}{} is used because it has 0 size. struct{} is a type with zero elements and struct{}{} creates an instance
		shutdownChan <- struct{}{}
	}()

	// Wait for soft context to be cancelled in main thread
	select {
	case <-shutdownChan:
		// If the worker loop signals before the soft context is cancelled, we want to log that it has finished earlier than expected
		logger.
			WithField("event", "error.feed_api_worker.worker_early_shutdown").
			Error("Worker has finished before soft context was cancelled")
		return
	case <-softCtx.Done():
		// In shutdown phase; wait a bit before doing `hardCancel`
		// In this phase we wait for a new process in another pod to pick up for the current worker on the next heartbeat
		// After timeout we cancel the hard context and the worker will stop picking up work
		// NB: Hard cancel abruptly shuts down the running process.
		// If the process should stop before hard cancel is triggered another context should be used for this signal

		select {
		case <-time.After(timeout):
			hardCancel()
			select {
			case <-shutdownChan:
				// Should still wait for shutdownChan to be called to avoid goroutine call closed channel
				logger.
					WithField("event", "info.feed_api_worker.worker_hard_shutdown").
					Info("Worker has finished after hard cancel")
				return
			}
		case <-shutdownChan:
			// Successfully completed closing worker
			logger.
				WithField("event", "info.feed_api_worker.worker_clean_shutdown").
				Info("Worker has finished after soft cancel")
			return
		}
	}
}
