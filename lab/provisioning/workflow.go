package provisioning

import (
	"time"

	lab "github.com/domderen/go-kubernetes-infrastructure-tutorial/lab"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const ReserveHostsActivity = "ReserveHosts"

type Request struct {
	ClusterUID string `json:"clusterUID"`
	NodeCount  int    `json:"nodeCount"`
}

// ProvisionCluster is the first reservation-only workflow increment.
// Imaging, joining, validation, and Kubernetes integration are later lessons.
func ProvisionCluster(ctx workflow.Context, req Request) ([]lab.HostAllocation, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout:    5 * time.Second,
		ScheduleToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    100 * time.Millisecond,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Second,
			MaximumAttempts:    3,
		},
	})
	var hosts []lab.HostAllocation
	// The request identity is unchanged across activity attempts. Provider I/O
	// belongs in the activity; workflow replay resolves recorded completions.
	if err := workflow.ExecuteActivity(ctx, ReserveHostsActivity, req).Get(ctx, &hosts); err != nil {
		return nil, err
	}
	return hosts, nil
}
