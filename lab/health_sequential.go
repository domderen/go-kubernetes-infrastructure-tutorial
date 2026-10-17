package lab

import "context"

// CheckHostsSequential checks hosts one at a time in input order.
// Per-host errors are retained; parent cancellation returns partial results.
// The supplied check must be non-nil and honor ctx for prompt cancellation.
func CheckHostsSequential(
	ctx context.Context,
	hosts []string,
	check func(context.Context, string) error,
) ([]HostResult, error) {
	var results []HostResult

	for _, host := range hosts {
		if err := ctx.Err(); err != nil {
			return results, err
		}

		checkErr := check(ctx, host)
		results = append(results, HostResult{
			Host: host,
			Err:  checkErr,
		})
	}

	return results, ctx.Err()
}
