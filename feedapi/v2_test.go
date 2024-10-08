package feedapi

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAPI_V2_HappyDay_Smoketest(t *testing.T) {
	server := Server(NewTestFeedAPI())

	client := createClientWithPartitionCount(server, NoV1Support)

	info, err := client.Discover(context.Background())
	require.NoError(t, err)

	assert.Equal(t, "the-token", info.Token)
	assert.Equal(t, 2, len(info.Partitions))

	var page EventPageSingleType[TestEvent]
	err = client.FetchEvents(context.Background(), info.Token, info.Partitions[0].Id, "9998", &page, Options{})
	require.NoError(t, err)

	assert.Equal(t, TestEvent{
		ID:      "00000000-0000-0000-0000-00000000270f",
		Version: 0,
		Cursor:  9999,
		Type:    "PaymentCaptured",
	}, page.Events[0])
}

type mockFeedInfo struct {
	info FeedInfo
}

func (m mockFeedInfo) GetName() string {
	return "mockFeedInfo"
}

func (m mockFeedInfo) GetFeedInfo() FeedInfo {
	return m.info
}

func (m mockFeedInfo) FetchEvents(ctx context.Context, token string, partitionID int, cursor string, receiver EventReceiver, options Options) error {
	panic("unexpected")
}

func TestDiscoverEndpoint(t *testing.T) {
	// No parameters, so rather simple test, just check the struct is marshalled through
	info := FeedInfo{
		Token: "some-token",
		Partitions: []Partition{
			{
				Id:     23423,
				Closed: true,
			},
			{
				Id:                   4543252,
				StartsAfterPartition: 23423,
			},
			{
				Id:                   83223,
				CursorFromPartitions: []int{23423, 4543252},
			},
		},
		ExactlyOnce: true,
	}

	server := Server(mockFeedInfo{info})
	client := createClientWithPartitionCount(server, NoV1Support)
	gotInfo, err := client.Discover(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, info, gotInfo)
}

func TestEventsEndpoint(t *testing.T) {
	server := Server(NewTestFeedAPI())
	tests := []struct {
		name string

		token        string
		pageSizeHint int
		EventTypes   []string
		partitionID  int
		cursor       string

		expectedEvents      int
		expectedErrorString string
	}{
		{
			name:                "token mismatch",
			token:               "wrong-token",
			partitionID:         0,
			cursor:              "qwerty",
			expectedErrorString: ErrRediscoveryNeeded.Error(),
		},
		{
			name:                "wrong cursor",
			token:               "the-token",
			partitionID:         0,
			cursor:              "qwerty",
			expectedErrorString: "response code 500, response body: Internal server error\n",
		},
		{
			name:           "out of range cursor",
			token:          "the-token",
			partitionID:    0,
			cursor:         "20000",
			expectedEvents: 0,
		},
		{
			name:           "_first special cursor",
			token:          "the-token",
			partitionID:    0,
			cursor:         FirstCursor,
			expectedEvents: 100,
		},
		{
			name:           "_last special cursor",
			token:          "the-token",
			partitionID:    0,
			cursor:         LastCursor,
			expectedEvents: 1,
		},
		{
			name:           "pagesizehint 10000, full page",
			token:          "the-token",
			pageSizeHint:   10000,
			partitionID:    0,
			cursor:         FirstCursor,
			expectedEvents: 10000,
		},
		{
			name:           "pagesizehint 10000, half page",
			token:          "the-token",
			pageSizeHint:   10000,
			partitionID:    0,
			cursor:         "4999",
			expectedEvents: 5000,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var page EventPageSingleType[TestEvent]

			client := createClientWithPartitionCount(server, NoV1Support)
			err := client.FetchEvents(context.Background(), test.token, test.partitionID, test.cursor, &page, Options{
				PageSizeHint: test.pageSizeHint,
				EventTypes:   test.EventTypes,
			})
			if err == nil {
				require.Equal(t, test.expectedEvents, len(page.Events))
			} else {
				require.Equal(t, test.expectedErrorString, err.Error())
			}
		})
	}
}

/*
Given 10.000 events, 5000 per partition.
Half of each is payment captured, and half is payment cancelled.
*/
func TestEventsEndpoint_Given_Optional_EventFilter(t *testing.T) {
	server := Server(NewTestFeedAPI())
	tests := []struct {
		name           string
		EventTypes     []string
		expectedEvents int
	}{
		{
			name:           "some matched events",
			EventTypes:     []string{"PaymentCaptured"},
			expectedEvents: 5000,
		},
		{
			name:           "all events matched",
			EventTypes:     []string{"PaymentCaptured", "PaymentCancelled"},
			expectedEvents: 10000,
		},
		{
			name:           "no matched events 1",
			EventTypes:     []string{"Unknown Event Type"},
			expectedEvents: 0,
		},
		{
			name:           "no matched events 2",
			EventTypes:     []string{},
			expectedEvents: 0,
		},
		{
			name:           "all events matched by default",
			EventTypes:     nil,
			expectedEvents: 10000,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var page EventPageSingleType[TestEvent]

			client := createClientWithPartitionCount(server, NoV1Support)
			_ = client.FetchEvents(context.Background(), "the-token", 0, FirstCursor, &page, Options{
				PageSizeHint: 10000,
				EventTypes:   test.EventTypes,
			})

			require.Equal(t, test.expectedEvents, len(page.Events))
		})
	}
}
