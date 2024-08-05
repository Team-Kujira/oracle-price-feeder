package types

import "time"

type CosmosTx struct {
	Code   int64
	Height uint64
	Time   time.Time
	Events []CosmosTxEvent
	Hash   string
}

func (tx *CosmosTx) GetEventsByType(eventType string) []CosmosTxEvent {
	events := []CosmosTxEvent{}
	for _, event := range tx.Events {
		if event.Type == eventType {
			events = append(events, event)
		}
	}

	return events
}

type CosmosTxEvent struct {
	Type       string
	Attributes map[string]string
}
