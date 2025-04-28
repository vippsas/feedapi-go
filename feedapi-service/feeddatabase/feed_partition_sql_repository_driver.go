package feeddatabase

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/microsoft/go-mssqldb"
	"github.com/sirupsen/logrus"
	"time"
)

const (
	feedEventName = "feed-partition-table"
)

var (
	CursorNotUpdatedError = errors.New("cursor not updated")
)

type FeedPartition struct {
	PartitionID int
	Cursor      string
	Closed      bool
}

type FeedInfoDatabase struct {
	DB                   *sql.DB
	updateCursorQuery    *sql.Stmt
	fetchPartitionsQuery *sql.Stmt
	sqlTimeout           time.Duration
}

func Init(DB *sql.DB, sqlTimeout time.Duration) (FeedPartitionSqlRepository, error) {
	dbManager := &FeedInfoDatabase{
		DB:         DB,
		sqlTimeout: sqlTimeout,
	}

	stmt, err := dbManager.DB.Prepare(QUpdateCursor)
	if err != nil {
		return nil, err
	}
	dbManager.updateCursorQuery = stmt

	stmt, err = dbManager.DB.Prepare(QFetchPartitions)
	if err != nil {
		return nil, err
	}
	dbManager.fetchPartitionsQuery = stmt

	return dbManager, nil
}

func (db *FeedInfoDatabase) UpdateCursor(ctx context.Context, logger logrus.FieldLogger, partitionID int, cursor string) error {
	ctx, cancel := context.WithTimeout(ctx, db.sqlTimeout)
	defer cancel()

	logger.
		WithField("event", fmt.Sprintf("debug.%s.update_cursor_transaction_started", feedEventName)).
		Debug("Initialized update cursor")

	transaction, err := db.DB.Begin()
	if err != nil {
		return err
	}

	rows, err := db.updateCursorQuery.ExecContext(
		ctx,
		sql.Named("partitionId", partitionID),
		sql.Named("cursor", mssql.VarChar(cursor)),
	)

	if err != nil {
		if errRb := transaction.Rollback(); errRb != nil {
			logger.
				WithError(errRb).
				WithField("event", fmt.Sprintf("error.%s.rollback_update_cursor_transaction_failed", feedEventName)).
				Error("Failed rollback transaction")
		}
		return err
	}

	if err = transaction.Commit(); err != nil {
		return err
	}

	// Validate that the cursor was updated, otherwise this will go unnoticed
	affectedRows, err := rows.RowsAffected()
	if err != nil {
		return err
	}

	if affectedRows == 0 {
		return CursorNotUpdatedError
	}

	logger.
		WithField("event", fmt.Sprintf("debug.%s.update_cursor_transaction_successful", feedEventName)).
		Debug("Successfully committed cursor update")

	return nil
}

func (db *FeedInfoDatabase) FetchPartitions(ctx context.Context) (map[int]FeedPartition, error) {
	rows, err := db.fetchPartitionsQuery.QueryContext(ctx)

	if err != nil {
		return nil, err
	}

	return mapFeedInfo(rows)
}

func mapFeedInfo(from *sql.Rows) (map[int]FeedPartition, error) {
	partitions := make(map[int]FeedPartition)
	for from.Next() {
		var partition FeedPartition
		if err := from.Scan(&partition.PartitionID, &partition.Cursor, &partition.Closed); err != nil {
			return nil, err
		}
		partitions[partition.PartitionID] = partition
	}
	return partitions, nil
}
