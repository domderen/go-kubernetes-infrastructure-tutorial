// provider runs the durable local simulator. It never provisions real hardware.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/domderen/go-kubernetes-infrastructure-tutorial/lab/mockprovider"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:9080", "local provider HTTP address")
	state := flag.String("state", "../.local/provider/state.json", "persistent simulator state file")
	delay := flag.Duration("image-delay", 6*time.Second, "simulated imaging duration")
	flag.Parse()
	provider, err := mockprovider.New(*state, *delay)
	if err != nil {
		slog.Error("open provider state", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	server := &http.Server{Addr: *listen, Handler: provider.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	slog.Info("simulated provider listening", "address", *listen, "state", *state)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("provider server", "error", err)
		os.Exit(1)
	}
}
