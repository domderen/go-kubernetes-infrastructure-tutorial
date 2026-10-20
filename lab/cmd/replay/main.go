// replay downloads real event history and checks the current workflow offline.
// It registers no activities and makes no provider calls.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/domderen/go-kubernetes-infrastructure-tutorial/lab/provisioning"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	temporallog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/encoding/protojson"
)

func main() {
	historyFile := flag.String("history", "", "replay an existing history JSON file offline")
	workflowID := flag.String("workflow-id", "", "workflow history to download")
	runID := flag.String("run-id", "", "exact run to download")
	address := flag.String("temporal-address", "127.0.0.1:7233", "Temporal frontend")
	namespace := flag.String("temporal-namespace", "default", "Temporal namespace")
	output := flag.String("out", "../.local/live/history.json", "where to save downloaded history")
	flag.Parse()
	history := &historypb.History{}
	if *historyFile != "" {
		data, err := os.ReadFile(*historyFile)
		check(err)
		check(protojson.Unmarshal(data, history))
	} else {
		if *workflowID == "" || *runID == "" {
			check(fmt.Errorf("provide --history, or both --workflow-id and --run-id"))
		}
		c, err := client.Dial(client.Options{HostPort: *address, Namespace: *namespace})
		check(err)
		defer c.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		iterator := c.GetWorkflowHistory(ctx, *workflowID, *runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for iterator.HasNext() {
			event, err := iterator.Next()
			check(err)
			history.Events = append(history.Events, event)
		}
		data, err := (protojson.MarshalOptions{Indent: "  "}).Marshal(history)
		check(err)
		check(os.MkdirAll(filepath.Dir(*output), 0o755))
		check(os.WriteFile(*output, data, 0o600))
		fmt.Printf("Saved %d real history events to %s\n", len(history.Events), *output)
	}
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflow(provisioning.ProvisionCluster)
	check(replayer.ReplayWorkflowHistory(temporallog.NewStructuredLogger(slog.Default()), history))
	fmt.Printf("Replay passed: %d events, no activities registered or provider calls made.\n", len(history.Events))
}

func check(err error) {
	if err != nil {
		slog.Error("replay", "error", err)
		os.Exit(1)
	}
}
