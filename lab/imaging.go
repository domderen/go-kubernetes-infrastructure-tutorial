package lab

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrInvalidImageRequest  = errors.New("owner UID, operation ID, host ID, and image version are required")
	ErrImageRequestConflict = errors.New("image operation ID already belongs to a different request")
	ErrHostNotOwned         = errors.New("host is not allocated to this owner UID")
	ErrImageJobNotFound     = errors.New("image job was not found for this owner UID")
)

// ImageRequest contains identity known before a provider job exists.
// OperationID identifies one logical imaging request and survives retries.
// HostID and ImageVersion must stay unchanged for that operation.
type ImageRequest struct {
	OwnerUID     string
	OperationID  string
	HostID       string
	ImageVersion string
}

// ImageJob records a provider request and its simulated completion observation.
type ImageJob struct {
	ID      string
	Request ImageRequest
	Done    bool
}

type imageOperationKey struct {
	ownerUID    string
	operationID string
}

// EnsureImageJob atomically finds or creates the job for an owner/operation key.
// The caller does not need a previously returned job ID or heartbeat checkpoint.
func (p *SimulatedAllocator) EnsureImageJob(ctx context.Context, req ImageRequest) (ImageJob, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ImageJob{}, err
	}
	if req.OwnerUID == "" || req.OperationID == "" || req.HostID == "" || req.ImageVersion == "" {
		return ImageJob{}, ErrInvalidImageRequest
	}
	if p.releasedOwners[req.OwnerUID] {
		return ImageJob{}, ErrOwnerReleased
	}
	owned := false
	for _, host := range p.hosts {
		if host.OwnerUID == req.OwnerUID && host.ID == req.HostID {
			owned = true
			break
		}
	}
	if !owned {
		return ImageJob{}, ErrHostNotOwned
	}

	key := imageOperationKey{ownerUID: req.OwnerUID, operationID: req.OperationID}
	if job, found := p.imageJobs[key]; found {
		if job.Request != req {
			return ImageJob{}, ErrImageRequestConflict
		}
		return job, nil
	}
	if p.imageJobs == nil {
		p.imageJobs = make(map[imageOperationKey]ImageJob)
	}
	p.nextImageID++
	job := ImageJob{ID: fmt.Sprintf("image-%03d", p.nextImageID), Request: req}
	p.imageJobs[key] = job
	return job, nil
}

// GetImageJob observes a job owned by this UID. Closed owners stop polling.
func (p *SimulatedAllocator) GetImageJob(ctx context.Context, ownerUID, jobID string) (ImageJob, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ImageJob{}, err
	}
	if p.releasedOwners[ownerUID] {
		return ImageJob{}, ErrOwnerReleased
	}
	for _, job := range p.imageJobs {
		if job.Request.OwnerUID == ownerUID && job.ID == jobID {
			return job, nil
		}
	}
	return ImageJob{}, ErrImageJobNotFound
}

// CompleteImageJob is the simulated provider's completion event, not a cancel
// or imaging implementation. Already accepted work can finish after cleanup.
func (p *SimulatedAllocator) CompleteImageJob(ctx context.Context, ownerUID, jobID string) (ImageJob, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ImageJob{}, err
	}
	for key, job := range p.imageJobs {
		if job.Request.OwnerUID == ownerUID && job.ID == jobID {
			job.Done = true
			p.imageJobs[key] = job
			return job, nil
		}
	}
	return ImageJob{}, ErrImageJobNotFound
}
