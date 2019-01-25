package processor

import (
	"encoding/json"

	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"

	"github.com/joincivil/go-common/pkg/pubsub"
)

func (e *EventProcessor) pubSub(event *crawlermodel.Event) error {
	if !e.pubsubEnabled() {
		return nil
	}

	// NOTE(IS): We only want to send notifications on watched events
	if event.RetrievalMethod() == crawlermodel.Filterer {
		return nil
	}

	payload, err := e.pubSubBuildPayload(event)
	if err != nil {
		return err
	}

	return e.googlePubSub.Publish(payload)
}

// PubSubMessage is a struct that represents a message to be published to the pubsub.
type PubSubMessage struct {
	TxHash string `json:"txHash"`
}

func (e *EventProcessor) pubSubBuildPayload(event *crawlermodel.Event) (*pubsub.GooglePubSubMsg, error) {
	msg := &PubSubMessage{TxHash: event.TxHash().Hex()}

	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	googlePubSubMsg := &pubsub.GooglePubSubMsg{
		Topic:   e.googlePubSubTopicName,
		Payload: string(msgBytes),
	}

	return googlePubSubMsg, nil
}

func (e *EventProcessor) pubsubEnabled() bool {
	if e.googlePubSub == nil {
		return false
	}
	if e.googlePubSubTopicName == "" {
		return false
	}
	return true
}
