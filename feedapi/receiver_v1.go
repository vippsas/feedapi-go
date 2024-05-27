package feedapi

import (
	"encoding/json"
	"io"
)

// NDJSONEventSerializerV1 implements EventReceiver by emitting Newline-Delimited-JSON to a writer.
// It adds a "partition" key to the respose structs, as used by the version 1 (ZeroEventHub) server
type NDJSONEventSerializerV1 struct {
	encoder     *json.Encoder
	writer      io.Writer
	partitionID int
}

func NewNDJSONEventSerializerV1(partitionID int, writer io.Writer) *NDJSONEventSerializerV1 {
	return &NDJSONEventSerializerV1{
		encoder:     json.NewEncoder(writer),
		writer:      writer,
		partitionID: partitionID,
	}
}

func (s NDJSONEventSerializerV1) writeNdJsonLine(item interface{}) error {
	return s.encoder.Encode(item)
}

func (s NDJSONEventSerializerV1) Checkpoint(cursor string) error {
	type CursorMessage struct {
		Cursor    string `json:"cursor"`
		Partition int    `json:"partition"`
	}
	return s.writeNdJsonLine(CursorMessage{Cursor: cursor, Partition: s.partitionID})
}

func (s NDJSONEventSerializerV1) Event(data json.RawMessage) error {
	type EventMessage struct {
		Data      json.RawMessage `json:"data"`
		Partition int             `json:"partition"`
	}
	return s.writeNdJsonLine(EventMessage{Data: data, Partition: s.partitionID})
}

var _ EventReceiver = &NDJSONEventSerializerV1{}
