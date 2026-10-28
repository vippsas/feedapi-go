package feedapi

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClientPageRead(t *testing.T) {
	for _, protocol := range []struct{ name, token string }{
		{"v1", V1Token}, {"v2", "token"},
	} {
		t.Run(protocol.name, func(t *testing.T) {
			for _, tc := range []struct {
				name         string
				payloadBytes int
				truncateBody bool
				wantError    bool
			}{
				{"large event", 128 * 1024, false, false},
				{"line exceeds limit", 9 * 1024 * 1024, false, true},
				{"incomplete response", 10, true, true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					// A prior event/checkpoint must not conceal a later stream failure.
					body := "{\"data\":{\"value\":\"first\"}}\n{\"cursor\":\"1\"}\n" +
						"{\"data\":{\"value\":\"" + strings.Repeat("x", tc.payloadBytes) + "\"}}\n"
					if !tc.truncateBody {
						body += "{\"cursor\":\"2\"}\n"
					}
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if tc.truncateBody {
							w.Header().Set("Content-Length", fmt.Sprint(len(body)+100))
						}
						_, _ = io.WriteString(w, body)
					}))
					t.Cleanup(server.Close)
					client := NewClient(server.URL, 1)
					var page EventPageSingleType[struct{ Value string }]
					err := client.FetchEvents(context.Background(), protocol.token, 0, "0", &page, Options{})
					if tc.wantError {
						require.Error(t, err)
						if tc.truncateBody {
							require.ErrorIs(t, err, io.ErrUnexpectedEOF)
						} else {
							require.Contains(t, err.Error(), "token too long")
						}
						require.Equal(t, "1", page.Cursor)
						return
					}
					require.NoError(t, err)
					require.Equal(t, "2", page.Cursor)
					require.Len(t, page.Events, 2)
					require.Equal(t, strings.Repeat("x", tc.payloadBytes), page.Events[1].Value)
				})
			}
		})
	}
}
