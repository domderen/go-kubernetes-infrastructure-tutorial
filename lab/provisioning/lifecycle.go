package provisioning

import (
	"context"
	"errors"

	lab "github.com/domderen/go-kubernetes-infrastructure-tutorial/lab"
	"go.temporal.io/sdk/temporal"
)

const (
	JoinClusterActivity  = "JoinCluster"
	ValidateGPUActivity  = "ValidateGPU"
	ReleaseHostsActivity = "ReleaseHosts"
)

// LifecycleProvider is the consumer's boundary for simulated host readiness and
// owner-scoped compensation. ReleaseHosts must be idempotent and fence late work.
type LifecycleProvider interface {
	JoinCluster(context.Context, lab.HostOperation) error
	ValidateGPU(context.Context, lab.HostOperation) error
	ReleaseHosts(context.Context, string) error
}

type LifecycleActivities struct {
	Provider LifecycleProvider
}

func (a *LifecycleActivities) JoinCluster(ctx context.Context, req lab.HostOperation) error {
	return lifecycleActivityError(a.Provider.JoinCluster(ctx, req))
}

func (a *LifecycleActivities) ValidateGPU(ctx context.Context, req lab.HostOperation) error {
	return lifecycleActivityError(a.Provider.ValidateGPU(ctx, req))
}

func (a *LifecycleActivities) ReleaseHosts(ctx context.Context, ownerUID string) error {
	return lifecycleActivityError(a.Provider.ReleaseHosts(ctx, ownerUID))
}

func lifecycleActivityError(err error) error {
	if errors.Is(err, lab.ErrUnsupportedGPUClass) || errors.Is(err, lab.ErrValidationFailed) ||
		errors.Is(err, lab.ErrHostNotOwned) || errors.Is(err, lab.ErrOwnerReleased) ||
		errors.Is(err, lab.ErrInvalidImageRequest) || errors.Is(err, lab.ErrImageRequestConflict) {
		return temporal.NewNonRetryableApplicationError(err.Error(), "LifecycleRejected", err)
	}
	return err
}
