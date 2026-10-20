package provisioning

import (
	"fmt"
	"time"

	lab "github.com/domderen/go-kubernetes-infrastructure-tutorial/lab"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const ReserveHostsActivity = "ReserveHosts"

type Request struct {
	ClusterUID string `json:"clusterUID"`
	NodeCount  int    `json:"nodeCount"`
	// OperationID identifies this provisioning intent, unchanged across retries.
	OperationID  string `json:"operationID"`
	ImageVersion string `json:"imageVersion"`
	GPUClass     string `json:"gpuClass"`
	MaxParallel  int    `json:"maxParallel"`
}

// ProvisionCluster reserves capacity, then images, joins, and validates hosts.
func ProvisionCluster(ctx workflow.Context, req Request) (hosts []lab.HostAllocation, resultErr error) {
	if req.OperationID == "" || req.ImageVersion == "" {
		return nil, temporal.NewNonRetryableApplicationError("operation ID and image version are required", "InvalidRequest", nil)
	}
	if req.GPUClass == "" {
		req.GPUClass = "mock-a100"
	}
	if req.MaxParallel == 0 {
		req.MaxParallel = 1
	}
	if req.MaxParallel < 1 || req.MaxParallel > 10 {
		return nil, temporal.NewNonRetryableApplicationError("max parallel must be 1–10", "InvalidRequest", nil)
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout:    5 * time.Second,
		ScheduleToCloseTimeout: 30 * time.Second,
		WaitForCancellation:    true,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    100 * time.Millisecond,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Second,
			MaximumAttempts:    3,
		},
	})
	// Once a valid reservation can reach the provider, failure has an explicit
	// compensation obligation, including a lost response from a partial reserve.
	if req.ClusterUID != "" && req.NodeCount >= 0 && req.NodeCount <= 100 {
		defer func() {
			if resultErr == nil {
				return
			}
			cleanupCtx, cancelCleanup := workflow.NewDisconnectedContext(ctx)
			defer cancelCleanup()
			// Inherit the bounded short-activity options, but not cancellation.
			if err := workflow.ExecuteActivity(cleanupCtx, ReleaseHostsActivity, req.ClusterUID).Get(cleanupCtx, nil); err != nil {
				resultErr = fmt.Errorf("%w; cleanup failed: %v", resultErr, err)
			}
		}()
	}
	// The request identity is unchanged across activity attempts. Provider I/O
	// belongs in the activity; workflow replay resolves recorded completions.
	if err := workflow.ExecuteActivity(ctx, ReserveHostsActivity, req).Get(ctx, &hosts); err != nil {
		return nil, err
	}
	// Workflow coroutines execute deterministically. They share the next index;
	// only activity I/O runs concurrently outside the workflow interpreter.
	workers := min(req.MaxParallel, len(hosts))
	workCtx, cancelWorkers := workflow.WithCancel(ctx)
	defer cancelWorkers()
	next := 0
	var firstErr error
	group := workflow.NewWaitGroup(ctx)
	for range workers {
		group.Go(workCtx, func(workerCtx workflow.Context) {
			for next < len(hosts) && firstErr == nil && workerCtx.Err() == nil {
				host := hosts[next]
				next++
				if err := provisionHost(workerCtx, req, host); err != nil {
					if firstErr == nil {
						firstErr = err
						cancelWorkers()
					}
					return
				}
			}
		})
	}
	// WaitForCancellation keeps activity futures pending until acknowledgment
	// or timeout. Join every worker before the deferred compensation can run.
	waitCtx, cancelWait := workflow.NewDisconnectedContext(ctx)
	defer cancelWait()
	group.Wait(waitCtx)
	if firstErr != nil {
		return nil, firstErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return hosts, nil
}

func provisionHost(ctx workflow.Context, req Request, host lab.HostAllocation) error {
	imageCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout:    30 * time.Minute,
		ScheduleToCloseTimeout: time.Hour,
		HeartbeatTimeout:       30 * time.Second,
		WaitForCancellation:    true,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    100 * time.Millisecond,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Second,
			MaximumAttempts:    3,
		},
	})
	imageReq := lab.ImageRequest{
		OwnerUID:     req.ClusterUID,
		OperationID:  fmt.Sprintf("%s/image/%d", req.OperationID, host.Slot),
		HostID:       host.ID,
		ImageVersion: req.ImageVersion,
	}
	if err := workflow.ExecuteActivity(imageCtx, EnsureImageActivity, imageReq).Get(imageCtx, nil); err != nil {
		return err
	}
	operation := lab.HostOperation{OwnerUID: req.ClusterUID, HostID: host.ID, GPUClass: req.GPUClass}
	operation.OperationID = fmt.Sprintf("%s/join/%d", req.OperationID, host.Slot)
	if err := workflow.ExecuteActivity(ctx, JoinClusterActivity, operation).Get(ctx, nil); err != nil {
		return err
	}
	operation.OperationID = fmt.Sprintf("%s/validate/%d", req.OperationID, host.Slot)
	return workflow.ExecuteActivity(ctx, ValidateGPUActivity, operation).Get(ctx, nil)
}
