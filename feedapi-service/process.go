package feedapi_service

import (
	"context"
	"feedapi-service/eventprocessor"
	"feedapi-service/feeddatabase"
	"feedapi-service/feedmanager"
	"github.com/Unleash/unleash-client-go/v3"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
	"github.com/vippsas/feedapi-go/feedapi"
	"github.com/vippsas/go-leaderelection"
	"github.com/vippsas/verified-email/app/feature_toggle"
	"github.com/vippsas/verified-email/library/strict_utc_time"
	"net/http"
	"time"
)

var (
	noUpdateErr = errors.New("no new data")
)

var (
	readFeedApiGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "read_feed_api_gauge",
		Help: "The last time feed api was pulled in seconds since 1 january 1970",
	})
)

type FeedApiProcess struct {
	feedApiClient    feedapi.Client
	partitionManager feedmanager.PartitionManager
	eventProcessor   eventprocessor.EventProcessor
	unleash          feature_toggle.UnleashClient
	pullInterval     time.Duration
	backOffInterval  time.Duration
	pageSize         int
	finalContext     context.Context
}

func getLogger(ctx context.Context) logrus.FieldLogger {
	logger, _ := leaderelection.LoggerFromContext(ctx)
	return logger.WithFields(logrus.Fields{
		"Handler": "FeedApiProcessor",
	})
}

func CreateFeedApiProcess(
	ctx context.Context,
	feedInfoDBManager feeddatabase.FeedPartitionSqlRepository,
	eventProcessor eventprocessor.EventProcessor,
	unleash feature_toggle.UnleashClient,
	serviceUrl string,
	feedPullInterval time.Duration,
	feedBackOffInterval time.Duration,
	pageSize int,
	requestProcessor func(r *http.Request) error) (FeedApiProcess, error) {

	processLogger := getLogger(ctx)
	feedApiClient := feedapi.NewClient(serviceUrl, feedapi.NoV1Support).
		WithLogger(processLogger).
		WithRequestProcessor(requestProcessor)

	partitionManager := feedmanager.NewPartitionManager(feedInfoDBManager)

	return FeedApiProcess{
		feedApiClient:    feedApiClient,
		partitionManager: partitionManager,
		eventProcessor:   eventProcessor,
		unleash:          unleash,
		pullInterval:     feedPullInterval,
		backOffInterval:  feedBackOffInterval,
		pageSize:         pageSize,
		finalContext:     ctx,
	}, nil
}

func (process *FeedApiProcess) ConsumeFeedApi(ctx context.Context) {
	processLogger := getLogger(ctx)

	for {
		select {
		case <-ctx.Done():
			processLogger.
				WithField("event", "info.user_feed_api.context_done").
				Infof("Process context is done, exiting")
			// If the process context is done, this can mean the process was stolen by another worker, we should exit
			return
		case <-process.finalContext.Done():
			processLogger.
				WithField("event", "info.user_feed_api.final_context_done").
				Infof("Final context is done, exiting")
			// If the final context is done, we should exit
			return
		default:
			processLogger.
				WithField("event", "info.user_feed_api.starting_feed_consumer").
				Infof("Starting feed consumer")
			process.feedLoop(ctx)

			processLogger.
				WithField("event", "info.user_feed_api.feed_consumer_early_return").
				Infof("Feed consumer returned early, will back off %f seconds before restarting the consumer", process.backOffInterval.Seconds())

			time.Sleep(process.backOffInterval)
		}
	}
}

func (process *FeedApiProcess) feedLoop(ctx context.Context) {
	processLogger := getLogger(ctx)

	defer func() {
		if rc := recover(); rc != nil {
			switch rc.(type) {
			case error:
				processLogger = processLogger.WithError(errors.WithStack(rc.(error)))
			default:
				// Do nothing
			}
			processLogger.
				WithField("event", "error.user_feed_api.panic_detected").
				WithField("PanicReason", rc).
				Error("Detected panic for feed api consumer!")
		}
	}()

	token, err := process.validateFeedAndGetToken(ctx)
	if err != nil {
		processLogger.
			WithError(err).
			WithField("event", "error.user_feed_api.validate_feed").
			Error("Failed to validate feed")
		return
	}

	ticker := time.NewTicker(process.pullInterval)
	defer ticker.Stop()
	for {
		select {
		case <-process.finalContext.Done():
			// This will stop our consumer before the leader election process is done
			// This is to avoid cutting of event processing when there is a hard cancel in the leader election library
			return
		case <-ticker.C:
			if process.unleash.IsEnabled(feature_toggle.EnableFeedApi.Value(), unleash.WithFallback(false)) {
				if token == "" {
					processLogger.
						WithField("event", "error.user_feed_api.empty_token").
						Error("Cannot consume feed without token, fetch discovery again!")
					return
				}

				if err = process.processFeed(ctx, token); err != nil {
					if errors.Is(err, noUpdateErr) {
						continue
					} else if errors.Is(err, feedapi.ErrRediscoveryNeeded) {
						processLogger.
							WithField("event", "error.user_feed_api.feed_re-discovery_needed").
							Error("Feed consumer failed due to re-discovery needed!")
						return
					}

					processLogger.
						WithField("event", "error.user_feed_api.feed_consumer_failed").
						Error("Feed consumer failed unexpectedly!")
					return
				}
			}
		}
	}
}

func (process *FeedApiProcess) processFeed(ctx context.Context, token string) error {
	processLogger := getLogger(ctx)

	// Fetch registered partitions and their latest cursor
	partitions, err := process.partitionManager.FetchActivePartitions(ctx)
	if err != nil {
		return err
	}

	partitionCursorMap := map[int]string{}
	for _, partition := range partitions {
		partitionCursorMap[partition.PartitionID] = partition.Cursor
	}

	for {
		handleReadGauge()
		for partitionId, partitionCursor := range partitionCursorMap {
			processLogger = processLogger.
				WithField("partition", partitionId).
				WithField("cursor", partitionCursor)

			var page feedapi.EventPageRaw
			err = process.feedApiClient.FetchEvents(ctx, token, partitionId, partitionCursor, &page, feedapi.Options{PageSizeHint: process.pageSize})
			if err != nil {
				processLogger.
					WithError(err).
					WithField("event", "error.user_feed_api.fetch_event").
					Error("Failed to fetch event")
				return err
			}

			if page.Cursor == partitionCursor || page.Cursor == "" {
				return noUpdateErr
			}

			for _, event := range page.Events {
				process.eventProcessor.Process(ctx, event)
			}

			// Store progress so far
			err = process.partitionManager.UpdateCursor(ctx, processLogger, partitionId, page.Cursor)
			if err != nil {
				if errors.Is(err, feeddatabase.CursorNotUpdatedError) {
					processLogger.
						WithField("event", "error.user_feed_api.cursor_not_updated").
						Errorf("Could not update cursor not updated for partition: %d, from %s to %s", partitionId, partitionCursor, page.Cursor)
				}
				return err
			}

			processLogger.
				WithField("event", "info.user_feed_api.cursor_updated").
				Infof("Updated cursor for partition: %d, from %s to %s", partitionId, partitionCursor, page.Cursor)

			partitionCursorMap[partitionId] = page.Cursor
		}
	}
}

func (process *FeedApiProcess) validateFeedAndGetToken(ctx context.Context) (string, error) {
	processLogger := getLogger(ctx)

	processLogger.
		WithField("event", "info.user_feed_api.discovery_request").
		Info("Fetch discovery info from user service")

	feedInfo, err := process.feedApiClient.Discover(ctx)
	if err != nil {
		return "", err
	}

	processLogger.
		WithField("event", "info.user_feed_api.discover_response").
		Infof("User service discover response: %+v", feedInfo)

	err = process.partitionManager.ValidateFeed(ctx, processLogger, feedInfo)
	if err != nil {
		return "", err
	}

	return feedInfo.Token, nil
}

func handleReadGauge() {
	timeNow := float64(strict_utc_time.TimeNowUTC().Unix()) // Number of seconds since January 1, 1970
	readFeedApiGauge.Set(timeNow)
}
