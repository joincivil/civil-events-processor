package processor

import (
	"encoding/json"

	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"

	"github.com/joincivil/go-common/pkg/pubsub"
)

func (e *EventProcessor) pubSub(event *crawlermodel.Event) error {

	payload, err := e.pubSubBuildPayload(event)

	if err != nil {
		return err
	}

	return e.googlePubSub.Publish(payload)
}

// PubSubMessage - This is a struct
type PubSubMessage struct {
	TxHash string `json:"txHash"`
}

// TODO(jorgelo): Put this in configuration.
const pubsubTopic = "governance-events-staging"

func (e *EventProcessor) pubSubBuildPayload(event *crawlermodel.Event) (*pubsub.GooglePubSubMsg, error) {

	msg := &PubSubMessage{TxHash: event.TxHash().Hex()}

	msgBytes, err := json.Marshal(msg)

	if err != nil {
		return nil, err
	}

	googlePubSubMsg := &pubsub.GooglePubSubMsg{Topic: pubsubTopic, Payload: string(msgBytes)}

	return googlePubSubMsg, nil
}
