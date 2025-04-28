package feeddatabase

import (
	"context"
	"database/sql"
	"errors"
	"github.com/franela/goblin"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlserver"
	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/file"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/vippsas/go-leaderelection"
	"net/url"
	"testing"
	"time"
)

// NB: This test requires docker images to be running
func TestFeedPartitionSqlRepository(t *testing.T) {
	g := goblin.Goblin(t)

	g.Describe("Test feed partition sql repository", func() {
		testLogger := logrus.New()

		DB, err := initDatabaseAndRunMigration("../../../library/db/migrations")
		if err != nil {
			panic(err)
		}

		feedPartitionRepository, err := Init(DB, 1*time.Second)
		if err != nil {
			panic(err)
		}

		g.BeforeEach(func() {
			_, _ = DB.Exec("DELETE FROM FEED_API.TBL_FEED_PARTITION")
		})

		g.After(func() {
			_, _ = DB.Exec("DELETE FROM FEED_API.TBL_FEED_PARTITION")
			_ = insertPartition(DB, FeedPartition{
				PartitionID: 0,
				Cursor:      "_first",
				Closed:      false,
			})
		})

		g.It("Insert feed partition for new partition", func() {
			ctx := context.Background()
			ctx = leaderelection.ContextWithLogger(ctx, testLogger)
			feedPartition := FeedPartition{
				PartitionID: 1,
				Cursor:      "cursor",
			}

			err = insertPartition(DB, feedPartition)
			assert.Nil(g, err)
		})

		g.It("Insert duplicate partition id is not possible", func() {
			ctx := context.Background()
			ctx = leaderelection.ContextWithLogger(ctx, testLogger)
			feedPartition := FeedPartition{
				PartitionID: 1,
				Cursor:      "cursor",
			}

			err = insertPartition(DB, feedPartition)
			assert.Nil(g, err)

			err = insertPartition(DB, feedPartition)
			assert.NotNil(g, err)
		})

		g.It("Fetch empty feed partition", func() {
			ctx := context.Background()
			ctx = leaderelection.ContextWithLogger(ctx, testLogger)

			feedPartitions, err := feedPartitionRepository.FetchPartitions(ctx)
			assert.Nil(g, err)
			assert.True(g, len(feedPartitions) == 0)
		})

		g.It("Fetch feed partition after insert", func() {
			ctx := context.Background()
			ctx = leaderelection.ContextWithLogger(ctx, testLogger)
			feedPartition := FeedPartition{
				PartitionID: 1,
				Cursor:      "cursor",
			}

			err = insertPartition(DB, feedPartition)
			assert.Nil(g, err)

			feedPartitions, err := feedPartitionRepository.FetchPartitions(ctx)
			assert.Nil(g, err)
			assert.Equal(g, 1, len(feedPartitions))
			assert.True(g, isFeedPartitionEqual(feedPartition, feedPartitions[feedPartition.PartitionID]))
		})

		g.It("Fetch by partition id returns feed partition when defined", func() {
			ctx := context.Background()
			ctx = leaderelection.ContextWithLogger(ctx, testLogger)
			feedPartition := FeedPartition{
				PartitionID: 1,
				Cursor:      "cursor",
			}

			err = insertPartition(DB, feedPartition)
			assert.Nil(g, err)

			feedPartitions, err := feedPartitionRepository.FetchPartitions(ctx)
			assert.Nil(g, err)
			assert.True(g, isFeedPartitionEqual(feedPartition, feedPartitions[feedPartition.PartitionID]))
		})

		g.It("Update cursor when partition is not defined gives error", func() {
			ctx := context.Background()
			ctx = leaderelection.ContextWithLogger(ctx, testLogger)
			partitionId := 1
			cursor := "cursor"

			err = feedPartitionRepository.UpdateCursor(ctx, testLogger, partitionId, cursor)
			assert.NotNil(g, err)
			assert.True(g, errors.Is(err, CursorNotUpdatedError))
		})

		g.It("Update cursor for partition", func() {
			ctx := context.Background()
			ctx = leaderelection.ContextWithLogger(ctx, testLogger)
			feedPartition := FeedPartition{
				PartitionID: 1,
				Cursor:      "cursor",
				Closed:      false,
			}

			err = insertPartition(DB, feedPartition)
			assert.Nil(g, err)

			newCursor := "newCursor"
			err = feedPartitionRepository.UpdateCursor(ctx, testLogger, feedPartition.PartitionID, newCursor)
			assert.Nil(g, err)

			feedPartitions, err := feedPartitionRepository.FetchPartitions(ctx)
			assert.Nil(g, err)
			assert.Equal(g, newCursor, feedPartitions[feedPartition.PartitionID].Cursor)
		})
	})
}

func isFeedPartitionEqual(expected FeedPartition, actual FeedPartition) bool {
	return expected.PartitionID == actual.PartitionID &&
		expected.Cursor == actual.Cursor
}

func insertPartition(DB *sql.DB, feedPartition FeedPartition) error {
	QInsertPartition := `
		INSERT INTO FEED_API.TBL_FEED_PARTITION (PARTITION_ID, PARTITION_CURSOR, PARTITION_CLOSED) 
        VALUES (@partitionId, @cursor, @closed)`

	stmt, err := DB.Prepare(QInsertPartition)
	if err != nil {
		return err
	}

	transaction, err := DB.Begin()
	if err != nil {
		return err
	}

	_, err = stmt.Exec(
		sql.Named("partitionId", feedPartition.PartitionID),
		sql.Named("cursor", mssql.VarChar(feedPartition.Cursor)),
		sql.Named("closed", feedPartition.Closed),
	)

	if err != nil {
		if errRb := transaction.Rollback(); errRb != nil {
			return errRb
		}
		return err
	}

	if err = transaction.Commit(); err != nil {
		return err
	}

	return nil
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

	sqlServer, err := sql.Open("sqlserver", dbUrl)
	if err != nil {
		return nil, err
	}

	driver, err := sqlserver.WithInstance(sqlServer, &sqlserver.Config{})
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
			return sqlServer, nil
		} else {
			return nil, err
		}
	}
	return sqlServer, nil
}
