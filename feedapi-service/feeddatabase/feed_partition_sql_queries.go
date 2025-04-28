package feeddatabase

const (
	QUpdateCursor = `
		UPDATE FEED_API.TBL_FEED_PARTITION SET PARTITION_CURSOR=@cursor 
                WHERE PARTITION_ID=@partitionId`
	QFetchPartitions = `
		SELECT PARTITION_ID, PARTITION_CURSOR, PARTITION_CLOSED
                FROM FEED_API.TBL_FEED_PARTITION`
)
