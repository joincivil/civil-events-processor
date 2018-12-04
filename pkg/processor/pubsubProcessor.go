package processor

import (
	"encoding/json"

	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"
	crawlerutils "github.com/joincivil/civil-events-crawler/pkg/utils"
)

func (e *EventProcessor) pubSub(event *crawlermodel.Event) error {

	googlePubSub, err := e.pubSubBuildPayload(event)

	if err != nil {
		return err
	}

	return e.googlePubSub.Publish(googlePubSub)

}

// PubSubMessage - This is a struct
type PubSubMessage struct {
	TxHash string `json:"txHash"`
}

const pubsubTopic = "goverance-events-staging"

func (e *EventProcessor) pubSubBuildPayload(event *crawlermodel.Event) (*crawlerutils.GooglePubSubMsg, error) {

	msg := &PubSubMessage{TxHash: event.TxHash().Hex()}

	msgBytes, err := json.Marshal(msg)

	if err != nil {
		return nil, err
	}

	googlePubSubMsg := &crawlerutils.GooglePubSubMsg{Topic: pubsubTopic, Payload: string(msgBytes)}

	return googlePubSubMsg, nil
}
