package lab

import "errors"

// HostOperation identifies one simulated lifecycle action across retries.
type HostOperation struct {
	OwnerUID    string
	OperationID string
	HostID      string
	GPUClass    string
}

var ErrUnsupportedGPUClass = errors.New("unsupported simulated GPU class")
var ErrValidationFailed = errors.New("simulated GPU validation failed")
