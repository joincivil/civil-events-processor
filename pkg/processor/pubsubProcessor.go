package processor

import (
	"encoding/json"
	"fmt"

	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"

	"github.com/joincivil/go-common/pkg/pubsub"
)

func (e *EventProcessor) pubSub(event *crawlermodel.Event) error {
	if e.googlePubSub == nil {
		return fmt.Errorf("Google pubsub not initialized, failing to publish msg")
	}
	if e.googlePubSubTopicName == "" {
		return fmt.Errorf("Pubsub topic name not initialized, failing to publish msg")
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
