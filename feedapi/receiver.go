package feedapi

import (
	"encoding/json"
	"io"
)

// NDJSONEventSerializer implements EventReceiver by emitting Newline-Delimited-JSON to a writer.
type NDJSONEventSerializer struct {
	encoder *json.Encoder
	writer  io.Writer
}

func NewNDJSONEventSerializer(writer io.Writer) *NDJSONEventSerializer {
	return &NDJSONEventSerializer{
		encoder: json.NewEncoder(writer),
		writer:  writer,
	}
}

func (s NDJSONEventSerializer) writeNdJsonLine(item interface{}) error {
	return s.encoder.Encode(item)
}

func (s NDJSONEventSerializer) Checkpoint(cursor string) error {
	type CursorMessage struct {
		Cursor string `json:"cursor"`
	}
	return s.writeNdJsonLine(CursorMessage{Cursor: cursor})
}

func (s NDJSONEventSerializer) Event(data json.RawMessage) error {
	type EventMessage struct {
		Data json.RawMessage `json:"data"`
	}
	return s.writeNdJsonLine(EventMessage{Data: data})
}

var _ EventReceiver = &NDJSONEventSerializer{}

// EventPageRaw implements EventReceiver by storing the events and new cursor in memory.
// The data is stored as json.RawMessage. See EventPageSingleType for a simple way
// to use a single struct.
type EventPageRaw struct {
	Events  []json.RawMessage
	Cursor string
}

func (page *EventPageRaw) Checkpoint(cursor string) error {
	page.Cursor = cursor
	return nil
}

func (page *EventPageRaw) Event(d json.RawMessage) error {
	page.Events = append(page.Events, d)
	return nil
}

// EventPageSingleType is like EventPageRaw, but parses the JSON into a single struct
// type. Useful if all the events on the feed have the same format.
type EventPageSingleType[T any] struct {
	Events []T
	Cursor string
}

func (page *EventPageSingleType[T]) Checkpoint(cursor string) error {
	page.Cursor = cursor
	return nil
}

func (page *EventPageSingleType[T]) Event(d json.RawMessage) error {
	var data T
	if err := json.Unmarshal(d, &data); err != nil {
		return err
	}
	page.Events = append(page.Events, data)
	return nil
}
