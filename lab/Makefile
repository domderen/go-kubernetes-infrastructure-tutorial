GO ?= go
export GOCACHE ?= $(abspath ../.local/go-build)
LOCALBIN := $(CURDIR)/bin
CONTROLLER_TOOLS_VERSION := v0.23.0
ENVTEST_TOOL_VERSION := v0.25.3-0.20261012174806-32759fab3119
ENVTEST_K8S_VERSION := 1.37.0
CONTROLLER_GEN := $(LOCALBIN)/controller-gen-$(CONTROLLER_TOOLS_VERSION)
SETUP_ENVTEST := $(LOCALBIN)/setup-envtest-$(ENVTEST_TOOL_VERSION)

.PHONY: test unit generate envtest

# Runs the Go tests AND API-server integration tests, including the race detector.
test: generate envtest
	KUBEBUILDER_ASSETS="$$($(SETUP_ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -i -p path)" $(GO) test -tags=integration -race -count=1 -timeout=120s ./...

# Fast tests, including Temporal's in-process suite; excludes API-server tests.
unit:
	$(GO) test -race -count=1 -timeout=10s ./...

generate: $(CONTROLLER_GEN)
	$(CONTROLLER_GEN) object crd paths=./api/... output:crd:artifacts:config=config/crd/bases

envtest: $(SETUP_ENVTEST)
	$(SETUP_ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path >/dev/null

$(CONTROLLER_GEN): | $(LOCALBIN)
	GOBIN=$(LOCALBIN) $(GO) install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)
	mv $(LOCALBIN)/controller-gen $@

$(SETUP_ENVTEST): | $(LOCALBIN)
	GOBIN=$(LOCALBIN) $(GO) install sigs.k8s.io/controller-runtime/tools/setup-envtest@$(ENVTEST_TOOL_VERSION)
	mv $(LOCALBIN)/setup-envtest $@

$(LOCALBIN):
	mkdir -p $@
