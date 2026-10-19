package provisioning

import (
	"context"
	"errors"
	"time"

	lab "github.com/domderen/go-kubernetes-infrastructure-tutorial/lab"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

const EnsureImageActivity = "EnsureImage"

// ImageProvider belongs at the activity's I/O boundary.
type ImageProvider interface {
	EnsureImageJob(context.Context, lab.ImageRequest) (lab.ImageJob, error)
	GetImageJob(context.Context, string, string) (lab.ImageJob, error)
}

type ImageCheckpoint struct {
	JobID  string
	HostID string
}

type ImagingActivities struct {
	Provider     ImageProvider
	PollInterval time.Duration
}

// EnsureImage recovers an existing job by request key, then observes completion.
// This activity is not wired into ProvisionCluster yet.
func (a *ImagingActivities) EnsureImage(ctx context.Context, req lab.ImageRequest) (lab.ImageJob, error) {
	if err := ctx.Err(); err != nil {
		return lab.ImageJob{}, err
	}
	// Provider idempotency is the recovery path even when no heartbeat arrived.
	job, err := a.Provider.EnsureImageJob(ctx, req)
	if err != nil {
		return lab.ImageJob{}, imageActivityError(err)
	}
	interval := a.PollInterval
	if interval <= 0 {
		interval = time.Second
	}
	for {
		if err := ctx.Err(); err != nil {
			return lab.ImageJob{}, err
		}
		// Report after observing the provider; no independent heartbeat goroutine
		// keeps a blocked provider observation looking responsive.
		activity.RecordHeartbeat(ctx, ImageCheckpoint{JobID: job.ID, HostID: req.HostID})
		if job.Done {
			return job, nil
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return lab.ImageJob{}, ctx.Err()
		case <-timer.C:
		}
		job, err = a.Provider.GetImageJob(ctx, req.OwnerUID, job.ID)
		if err != nil {
			return lab.ImageJob{}, imageActivityError(err)
		}
	}
}

func imageActivityError(err error) error {
	if errors.Is(err, lab.ErrInvalidImageRequest) || errors.Is(err, lab.ErrImageRequestConflict) ||
		errors.Is(err, lab.ErrHostNotOwned) || errors.Is(err, lab.ErrOwnerReleased) {
		return temporal.NewNonRetryableApplicationError(err.Error(), "ImageRequestRejected", err)
	}
	return err
}
