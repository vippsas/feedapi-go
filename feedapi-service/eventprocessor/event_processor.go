package eventprocessor

import (
	"context"
	"encoding/json"
)

type EventProcessor interface {
	Process(context.Context, json.RawMessage)
}
