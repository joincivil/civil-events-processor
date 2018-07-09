// Package gen contains all the components for code generation.
package gen

import (
	"bytes"
	log "github.com/golang/glog"
	"go/format"
	"io"
	"sort"
	"strings"
	"text/template"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"

	crawlermodel "github.com/joincivil/civil-events-crawler/pkg/model"
)

// EventListContractTmplData represents the events for each supported contract
type EventListContractTmplData struct {
	Name       string
	EventNames []string
}

// EventListTmplData represents the data passed to the EventList template
type EventListTmplData struct {
	PackageName string
	GenTime     time.Time
	Contracts   []*EventListContractTmplData
}

// GenerateEventLists generates the code that represents the list of event names
// for each relevant contract
func GenerateEventLists(writer io.Writer, packageName string) error {
	contracts := []*EventListContractTmplData{}
	for _, t := range crawlermodel.ContractTypeToSpecs.Types() {
		spec, _ := crawlermodel.ContractTypeToSpecs.Get(t)
		_abi, err := loadAbiFromStr(spec.AbiStr())
		if err != nil {
			log.Errorf("Error loading ABI from string: err: %v", err)
			continue
		}
		eventNames := retrieveEventNamesFromAbi(_abi)
		contract := &EventListContractTmplData{
			Name:       spec.Name(),
			EventNames: eventNames,
		}
		contracts = append(contracts, contract)
	}
	tmplData := &EventListTmplData{
		PackageName: packageName,
		Contracts:   contracts,
		GenTime:     time.Now().UTC(),
	}
	return generate(writer, "eventslist.tmpl", handlerListTmpl, tmplData, true)
}

func retrieveEventNamesFromAbi(_abi *abi.ABI) []string {
	sortedEvents := eventsToSortedEventsSlice(_abi.Events)
	eventNames := make([]string, len(_abi.Events))
	for index, event := range sortedEvents {
		eventNames[index] = event.Name
	}
	return eventNames
}

func loadAbiFromStr(abiStr string) (*abi.ABI, error) {
	_abi, err := abi.JSON(strings.NewReader(abiStr))
	if err != nil {
		return nil, err
	}
	return &_abi, nil
}

func eventsToSortedEventsSlice(eventsMap map[string]abi.Event) []abi.Event {
	sortedEvents := make([]abi.Event, len(eventsMap))
	ind := 0
	for _, val := range eventsMap {
		sortedEvents[ind] = val
		ind++
	}
	sort.Sort(abiEventNameSort(sortedEvents))
	return sortedEvents
}

type abiEventNameSort []abi.Event

func (e abiEventNameSort) Len() int {
	return len(e)
}

func (e abiEventNameSort) Less(i, j int) bool {
	return e[i].Name < e[j].Name
}

func (e abiEventNameSort) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

func generate(writer io.Writer, tmplName string, tmpl string,
	tmplData interface{}, gofmt bool) error {
	t := template.Must(template.New(tmplName).Parse(tmpl))
	buf := &bytes.Buffer{}
	err := t.Execute(buf, tmplData)
	if err != nil {
		return err
	}
	output := buf.Bytes()
	if gofmt {
		output, err = format.Source(buf.Bytes())
		if err != nil {
			log.Errorf("ERROR Gofmt: err:%v\ntemplate generated code:\n%v", err, buf.String())
			return err
		}
	}
	_, err = writer.Write(output)
	return err
}
