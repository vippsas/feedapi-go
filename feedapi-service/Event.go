package feedapi_service

import (
	"github.com/goccy/go-json"
	"time"
)

type Event struct {
	SpecVersion     string          `json:"specVersion"`
	DataContentType string          `json:"dataContentType"`
	Type            string          `json:"type"`
	Source          string          `json:"source"`
	Subject         string          `json:"subject"`
	ID              string          `json:"id"`
	Time            time.Time       `json:"time"`
	Data            json.RawMessage `json:"data"`
}
