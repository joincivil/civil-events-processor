package processor

import (
	"github.com/davecgh/go-spew/spew"
	log "github.com/golang/glog"
	"github.com/lib/pq"
	"github.com/pkg/errors"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"

	"github.com/joincivil/civil-events-processor/pkg/model"

	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"

	cerrors "github.com/joincivil/go-common/pkg/errors"
	"github.com/joincivil/go-common/pkg/pubsub"
)

const (
	// Postgresql code indicating a unique_violation
	pqUniqueViolationCode = "23505"
)

func isStringInSlice(slice []string, target string) bool {
	for _, str := range slice {
		if target == str {
			return true
		}
	}
	return false
}

// NewEventProcessor is a convenience function to init an EventProcessor
func NewEventProcessor(params *NewEventProcessorParams) *EventProcessor {
	tcrEventProcessor := NewTcrEventProcessor(
		params.Client,
		params.ListingPersister,
		params.ChallengePersister,
		params.AppealPersister,
		params.GovEventPersister,
		params.UserChallengeDataPersister,
		params.PollPersister,
	)
	plcrEventProcessor := NewPlcrEventProcessor(
		params.Client,
		params.PollPersister,
		params.UserChallengeDataPersister,
		params.ChallengePersister,
	)
	newsroomEventProcessor := NewNewsroomEventProcessor(
		params.Client,
		params.ListingPersister,
		params.RevisionPersister,
	)
	cvlTokenProcessor := NewCvlTokenEventProcessor(
		params.Client,
		params.TokenTransferPersister,
	)
	parameterizerProcessor := NewParameterizerEventProcessor(
		params.Client,
		params.ChallengePersister,
		params.ParameterProposalPersister,
		params.PollPersister,
	)
	if params.ErrRep == nil {
		params.ErrRep = &cerrors.NullErrorReporter{}
	}
	return &EventProcessor{
		tcrEventProcessor:      tcrEventProcessor,
		plcrEventProcessor:     plcrEventProcessor,
		newsroomEventProcessor: newsroomEventProcessor,
		cvlTokenProcessor:      cvlTokenProcessor,
		parameterizerProcessor: parameterizerProcessor,
		googlePubSub:           params.GooglePubSub,
		googlePubSubTopicName:  params.GooglePubSubTopicName,
		errRep:                 params.ErrRep,
	}
}

// NewEventProcessorParams defines the params needed to be passed to the processor
type NewEventProcessorParams struct {
	Client                     bind.ContractBackend
	ListingPersister           model.ListingPersister
	RevisionPersister          model.ContentRevisionPersister
	GovEventPersister          model.GovernanceEventPersister
	ChallengePersister         model.ChallengePersister
	PollPersister              model.PollPersister
	AppealPersister            model.AppealPersister
	TokenTransferPersister     model.TokenTransferPersister
	ParameterProposalPersister model.ParamProposalPersister
	UserChallengeDataPersister model.UserChallengeDataPersister
	GooglePubSub               *pubsub.GooglePubSub
	GooglePubSubTopicName      string
	ErrRep                     cerrors.ErrorReporter
}

// EventProcessor handles the processing of raw events into aggregated data
// for use via the API.
type EventProcessor struct {
	tcrEventProcessor      *TcrEventProcessor
	plcrEventProcessor     *PlcrEventProcessor
	newsroomEventProcessor *NewsroomEventProcessor
	cvlTokenProcessor      *CvlTokenEventProcessor
	parameterizerProcessor *ParameterizerEventProcessor
	googlePubSub           *pubsub.GooglePubSub
	googlePubSubTopicName  string
	errRep                 cerrors.ErrorReporter
}

// Process runs the processor with the given set of raw CivilEvents
func (e *EventProcessor) Process(events []*crawlermodel.Event) error {
	var err error
	var ran bool

	if !e.pubsubEnabled() {
		log.Info("Events pubsub is disabled, to enable set the project ID and topic in the config.")
	}

	for _, event := range events {
		if log.V(2) {
			log.Infof("Process event: %v", spew.Sprintf("%#+v", event))
		}

		if event == nil {
			log.Errorf("Nil event found, should not be nil")
			e.errRep.Error(errors.New("nil event found"), nil)
			continue
		}

		ran, err = e.newsroomEventProcessor.Process(event)
		if err != nil {
			log.Errorf("Error processing newsroom event: err: %v\n", err)
			if !e.isAllowedErrProcess(err) {
				e.errRep.Error(err, nil)
			}
		}
		if ran {
			continue
		}

		ran, err = e.tcrEventProcessor.Process(event)
		if err != nil {
			log.Errorf("Error processing civil tcr event: err: %v\n", err)
			if !e.isAllowedErrProcess(err) {
				e.errRep.Error(err, nil)
			}
		}
		if ran {
			err = e.sendEventToPubsub(event)
			if err != nil {
				log.Errorf("Error publishing to pubsub: err %v\n", err)
				e.errRep.Error(err, nil)
			}
			continue
		}

		ran, err = e.plcrEventProcessor.Process(event)
		if err != nil {
			log.Errorf("Error processing plcr event: err: %v\n", err)
			if !e.isAllowedErrProcess(err) {
				e.errRep.Error(err, nil)
			}
		}
		if ran {
			continue
		}

		_, err = e.cvlTokenProcessor.Process(event)
		if err != nil {
			log.Errorf("Error processing token transfer event: err: %v\n", err)
			if !e.isAllowedErrProcess(err) {
				e.errRep.Error(err, nil)
			}
		}

		_, err = e.parameterizerProcessor.Process(event)
		if err != nil {
			log.Errorf("Error processing parameterizer event: err: %v\n", err)
		}
	}
	log.Info("Finished Processing")
	return err
}

func (e *EventProcessor) sendEventToPubsub(event *crawlermodel.Event) error {
	if !e.pubsubEnabled() {
		return nil
	}

	return e.pubSub(event)
}

// isAllowedErrProcess returns if an error should be ignored or not in the
// processing. This is used in the ensure we only report on
// particular errors and recover on others.
func (e *EventProcessor) isAllowedErrProcess(err error) bool {
	switch causeErr := errors.Cause(err).(type) {
	case *pq.Error:
		// Allow unique_violation errors
		if causeErr.Code == pqUniqueViolationCode {
			log.Infof("allowed *pq error code %v: %v, constraint: %v, msg: %v", causeErr.Code,
				causeErr.Code.Name(), causeErr.Constraint, causeErr.Message)
			return true
		}

	case pq.Error:
		// Allow unique_violation errors
		if causeErr.Code == pqUniqueViolationCode {
			log.Infof("allowed pq error code %v: %v, constraint: %v, msg: %v", causeErr.Code,
				causeErr.Code.Name(), causeErr.Constraint, causeErr.Message)
			return true
		}
	}

	return false
}
