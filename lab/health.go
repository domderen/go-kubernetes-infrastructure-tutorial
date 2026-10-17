package lab

// HostResult records the outcome of one attempted host check.
// Err is nil for a healthy host; a non-nil error does not abort the batch.
type HostResult struct {
	Host string
	Err  error
}
