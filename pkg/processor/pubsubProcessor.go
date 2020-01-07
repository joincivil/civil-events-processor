package processor

import (
	"encoding/json"

	log "github.com/golang/glog"
	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"

	"github.com/joincivil/go-common/pkg/pubsub"
)

func (e *EventProcessor) pubSub(event *crawlermodel.Event, topicName string) error {
	if !e.pubsubEnabled(topicName) {
		return nil
	}

	payload, err := e.pubSubBuildPayload(event, topicName)
	if err != nil {
		return err
	}

	log.Infof("Publishing to events pubsub: txhash: %v", event.TxHash().Hex())
	return e.googlePubSub.Publish(payload)
}

// PubSubMessage is a struct that represents a message to be published to the pubsub.
type PubSubMessage struct {
	TxHash string `json:"txHash"`
}

func (e *EventProcessor) pubSubBuildPayload(event *crawlermodel.Event,
	topicName string) (*pubsub.GooglePubSubMsg, error) {
	msg := &PubSubMessage{TxHash: event.TxHash().Hex()}

	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	googlePubSubMsg := &pubsub.GooglePubSubMsg{
		Topic:   topicName,
		Payload: string(msgBytes),
	}

	return googlePubSubMsg, nil
}

func (e *MultiSigEventProcessor) pubSubMultiSig(action string, ownerAddr string, multiSigAddr string, topicName string) error {
	if !e.pubsubEnabled(topicName) {
		return nil
	}

	payload, err := e.pubSubMultiSigBuildPayload(action, ownerAddr, multiSigAddr, topicName)
	if err != nil {
		return err
	}

	log.Infof("Publishing to events pubsub: action: %s ownerAddr: %s multiSigAddr: %s", action, ownerAddr, multiSigAddr)
	return e.googlePubSub.Publish(payload)
}

// PubSubMultiSigMessage is a struct that represents a message to be published to the pubsub relating to multi sigs.
type PubSubMultiSigMessage struct {
	Action       string `json:"action"`
	OwnerAddr    string `json:"ownerAddr"`
	MultiSigAddr string `json:"multiSigAddr"`
}

func (e *MultiSigEventProcessor) pubSubMultiSigBuildPayload(action string, ownerAddr string, multiSigAddr string,
	topicName string) (*pubsub.GooglePubSubMsg, error) {
	msg := &PubSubMultiSigMessage{Action: action, OwnerAddr: ownerAddr, MultiSigAddr: multiSigAddr}

	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	googlePubSubMsg := &pubsub.GooglePubSubMsg{
		Topic:   topicName,
		Payload: string(msgBytes),
	}

	return googlePubSubMsg, nil
}

func (e *EventProcessor) pubsubEnabled(topicName string) bool {
	if e.googlePubSub == nil {
		return false
	}
	if topicName == "" {
		return false
	}
	return true
}

func (e *MultiSigEventProcessor) pubsubEnabled(topicName string) bool {
	if e.googlePubSub == nil {
		return false
	}
	if topicName == "" {
		return false
	}
	return true
}
