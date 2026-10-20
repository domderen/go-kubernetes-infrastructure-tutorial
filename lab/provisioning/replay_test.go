package provisioning

import (
	"log/slog"
	"testing"

	temporallog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
)

// Captured from the real local Temporal service after killing/restarting the
// worker during imaging. Catches incompatible changes to this workflow's
// command sequence. No activities or provider are registered during replay.
func TestProvisionClusterReplaysRecordedHistory(t *testing.T) {
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflow(ProvisionCluster)
	if err := replayer.ReplayWorkflowHistoryFromJSONFile(temporallog.NewStructuredLogger(slog.Default()), "testdata/provision-cluster-history.json"); err != nil {
		t.Fatal(err)
	}
}
