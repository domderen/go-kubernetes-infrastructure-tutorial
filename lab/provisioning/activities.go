package provisioning

import (
	"context"
	"errors"

	lab "github.com/domderen/go-kubernetes-infrastructure-tutorial/lab"
	"go.temporal.io/sdk/temporal"
)

type Activities struct {
	Provider lab.HostAllocator
}

func (a *Activities) ReserveHosts(ctx context.Context, req Request) ([]lab.HostAllocation, error) {
	if req.ClusterUID == "" || req.NodeCount < 0 || req.NodeCount > 100 {
		return nil, temporal.NewNonRetryableApplicationError("cluster UID and node count 0–100 are required", "InvalidRequest", nil)
	}
	// Reuse provider discovery AND atomic (cluster UID, slot) ensures. A stale
	// list must not cause a duplicate allocation after a lost response.
	hosts, err := lab.ReconcileHosts(ctx, a.Provider, req.ClusterUID, req.NodeCount)
	if errors.Is(err, lab.ErrOwnerReleased) || errors.Is(err, lab.ErrScaleDownUnsupported) {
		return nil, temporal.NewNonRetryableApplicationError(err.Error(), "AllocationRejected", err)
	}
	return hosts, err
}
