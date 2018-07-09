package gen_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/joincivil/civil-events-processor/pkg/gen"
)

func TestGenerateEventLists(t *testing.T) {
	buf := &bytes.Buffer{}
	err := gen.GenerateEventLists(buf, "testpackage")
	if err != nil {
		t.Errorf("Should have not failed to generate event lists: err: %v", err)
	}

	code := buf.String()
	if !strings.Contains(code, "func IsValidCivilTCRContractEventName") {
		t.Error("Did not see expected IsValidCivilTCRContractEventName in the generated watcher code")
	}
}
