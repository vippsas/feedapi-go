package feedapi_service

import (
	"github.com/gofrs/uuid"
	"time"
)

type WorkerConfig struct {
	ProcessID                 string
	ProcessEffort             int
	ProcessNamespace          string
	WorkerID                  uuid.UUID
	Version                   string
	HeartbeatInterval         time.Duration
	WorkersNeededBeforeActive int
	ShutDownTimeout           time.Duration
	TimeoutPadding            time.Duration
}
