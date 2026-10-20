// Package mockprovider simulates provider-owned GPU host lifecycle state over
// HTTP. Exactly one Server process may own a state file at a time.
package mockprovider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	lab "github.com/domderen/go-kubernetes-infrastructure-tutorial/lab"
)

var (
	ErrInvalidOperation  = errors.New("owner UID, operation ID, host ID, and GPU class are required")
	ErrOperationConflict = errors.New("operation ID already belongs to a different request")
	ErrImageNotReady     = errors.New("host has no completed image job")
	ErrHostNotJoined     = errors.New("host has not joined the simulated cluster")
)

// Host is a snapshot of one simulated allocation and its lifecycle state.
type Host struct {
	ID        string `json:"id"`
	OwnerUID  string `json:"ownerUID"`
	Slot      int    `json:"slot"`
	Joined    bool   `json:"joined"`
	Validated bool   `json:"validated"`
}

func (h Host) allocation() lab.HostAllocation {
	return lab.HostAllocation{ID: h.ID, OwnerUID: h.OwnerUID, Slot: h.Slot}
}

// Image records accepted external work, including after owner cleanup. Done is
// observed from StartedAt and DelayMillis; restarting cannot restart its clock.
type Image struct {
	ID          string           `json:"id"`
	Request     lab.ImageRequest `json:"request"`
	StartedAt   time.Time        `json:"startedAt"`
	DelayMillis int64            `json:"delayMillis"`
	Done        bool             `json:"done"`
}

func (j Image) job(now time.Time) lab.ImageJob {
	return lab.ImageJob{ID: j.ID, Request: j.Request, Done: !now.Before(j.StartedAt.Add(time.Duration(j.DelayMillis) * time.Millisecond))}
}

// Counters count committed side effects, not requests or transient observations.
type Counters struct {
	HostsCreated            uint64 `json:"hostsCreated"`
	ImagesCreated           uint64 `json:"imagesCreated"`
	Joins                   uint64 `json:"joins"`
	Validations             uint64 `json:"validations"`
	Releases                uint64 `json:"releases"`
	LostAllocationResponses uint64 `json:"lostAllocationResponses"`
	LostImageResponses      uint64 `json:"lostImageResponses"`
}

// Event is a small bounded audit trail for the local recovery demonstrations.
type Event struct {
	At          time.Time `json:"at"`
	Kind        string    `json:"kind"`
	OwnerUID    string    `json:"ownerUID,omitempty"`
	HostID      string    `json:"hostID,omitempty"`
	OperationID string    `json:"operationID,omitempty"`
}

type faultConfig struct {
	LoseAllocationResponseOnce bool   `json:"loseAllocationResponseOnce"`
	LoseImageResponseOnce      bool   `json:"loseImageResponseOnce"`
	FailValidation             bool   `json:"failValidation"`
	ImageDelayMillis           *int64 `json:"imageDelayMillis,omitempty"`
}

// FaultRequest changes only supplied fields. An empty OwnerUID applies globally;
// an owner's image delay overrides the global delay for newly accepted jobs.
// Faults persist across provider restarts. Response-loss flags are consumed once
// after committing an ensure, and actually close that HTTP connection.
type FaultRequest struct {
	OwnerUID                   string `json:"ownerUID"`
	LoseAllocationResponseOnce *bool  `json:"loseAllocationResponseOnce,omitempty"`
	LoseImageResponseOnce      *bool  `json:"loseImageResponseOnce,omitempty"`
	FailValidation             *bool  `json:"failValidation,omitempty"`
	ImageDelayMillis           *int64 `json:"imageDelayMillis,omitempty"`
}

// Inventory exposes durable provider ownership rather than Kubernetes status.
// Jobs retain the existing lab.ImageRequest JSON field names (OwnerUID, etc.).
type Inventory struct {
	Hosts        []Host                 `json:"hosts"`
	Jobs         []Image                `json:"jobs"`
	ClosedOwners []string               `json:"closedOwners"`
	Counters     Counters               `json:"counters"`
	Events       []Event                `json:"events"`
	Faults       map[string]faultConfig `json:"faults"`
}

type operation struct {
	Kind    string            `json:"kind"`
	Request lab.HostOperation `json:"request"`
}

type diskState struct {
	Version      int                    `json:"version"`
	Hosts        []Host                 `json:"hosts"`
	Jobs         []Image                `json:"jobs"`
	ClosedOwners map[string]bool        `json:"closedOwners"`
	Operations   []operation            `json:"operations"`
	Counters     Counters               `json:"counters"`
	Events       []Event                `json:"events"`
	Faults       map[string]faultConfig `json:"faults"`
}

// Server serializes ownership checks and atomic file commits. Its mutex never
// protects remote work: all actions in this provider are deliberately simulated.
type Server struct {
	mu         sync.Mutex
	path       string
	imageDelay time.Duration
	state      diskState
}

// New opens or creates durable state. The caller must ensure no other provider
// process owns path; this teaching implementation is not a multi-writer database.
func New(path string, imageDelay time.Duration) (*Server, error) {
	if path == "" || imageDelay < 0 {
		return nil, errors.New("state path and nonnegative image delay are required")
	}
	s := &Server{path: path, imageDelay: imageDelay, state: diskState{Version: 1, ClosedOwners: map[string]bool{}, Faults: map[string]faultConfig{}}}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return nil, fmt.Errorf("create state directory: %w", err)
		}
		if err := s.persist(s.state); err != nil {
			return nil, err
		}
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read provider state: %w", err)
	}
	if err := json.Unmarshal(data, &s.state); err != nil {
		return nil, fmt.Errorf("decode provider state: %w", err)
	}
	if s.state.Version != 1 || s.state.ClosedOwners == nil || s.state.Faults == nil {
		return nil, errors.New("unsupported or incomplete provider state")
	}
	return s, nil
}

func (s *Server) persist(state diskState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode provider state: %w", err)
	}
	file, err := os.CreateTemp(filepath.Dir(s.path), ".provider-*.json")
	if err != nil {
		return fmt.Errorf("create temporary state: %w", err)
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("write provider state: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync provider state: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close provider state: %w", err)
	}
	if err := os.Rename(file.Name(), s.path); err != nil {
		return fmt.Errorf("replace provider state: %w", err)
	}
	return nil
}

// change publishes its private copy only after the atomic replacement succeeds.
// A caller can lose the response after this point without losing the side effect.
func (s *Server) change(ctx context.Context, fn func(*diskState) (any, error)) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	next := s.state
	next.Hosts = append([]Host(nil), next.Hosts...)
	next.Jobs = append([]Image(nil), next.Jobs...)
	next.Operations = append([]operation(nil), next.Operations...)
	next.Events = append([]Event(nil), next.Events...)
	next.ClosedOwners = make(map[string]bool, len(s.state.ClosedOwners))
	for key, closed := range s.state.ClosedOwners {
		next.ClosedOwners[key] = closed
	}
	next.Faults = make(map[string]faultConfig, len(s.state.Faults))
	for key, faults := range s.state.Faults {
		next.Faults[key] = faults
	}
	result, err := fn(&next)
	if err != nil {
		return nil, err
	}
	if err := s.persist(next); err != nil {
		return nil, err
	}
	s.state = next
	return result, nil
}

func (state *diskState) event(kind, ownerUID, hostID, operationID string) {
	state.Events = append(state.Events, Event{At: time.Now().UTC(), Kind: kind, OwnerUID: ownerUID, HostID: hostID, OperationID: operationID})
	const limit = 512
	if len(state.Events) > limit {
		state.Events = append([]Event(nil), state.Events[len(state.Events)-limit:]...)
	}
}

func (state *diskState) ownedHost(ownerUID, hostID string) (int, error) {
	if state.ClosedOwners[ownerUID] {
		return 0, lab.ErrOwnerReleased
	}
	for i, host := range state.Hosts {
		if host.OwnerUID == ownerUID && host.ID == hostID {
			return i, nil
		}
	}
	return 0, lab.ErrHostNotOwned
}

func (state *diskState) loseResponse(ownerUID, kind string) bool {
	for _, owner := range []string{ownerUID, ""} {
		faults := state.Faults[owner]
		if kind == "allocation" && faults.LoseAllocationResponseOnce {
			faults.LoseAllocationResponseOnce = false
			state.Faults[owner] = faults
			state.Counters.LostAllocationResponses++
			state.event("allocation-response-lost", ownerUID, "", "")
			return true
		}
		if kind == "image" && faults.LoseImageResponseOnce {
			faults.LoseImageResponseOnce = false
			state.Faults[owner] = faults
			state.Counters.LostImageResponses++
			state.event("image-response-lost", ownerUID, "", "")
			return true
		}
	}
	return false
}

type ensureResult struct {
	value any
	lost  bool
}

func (s *Server) ensureHost(ctx context.Context, ownerUID string, slot int) (any, error) {
	return s.change(ctx, func(state *diskState) (any, error) {
		if ownerUID == "" || slot < 1 {
			return nil, lab.ErrInvalidAllocation
		}
		if state.ClosedOwners[ownerUID] {
			return nil, lab.ErrOwnerReleased
		}
		for _, host := range state.Hosts {
			if host.OwnerUID == ownerUID && host.Slot == slot {
				return ensureResult{host.allocation(), state.loseResponse(ownerUID, "allocation")}, nil
			}
		}
		state.Counters.HostsCreated++
		host := Host{ID: fmt.Sprintf("host-%03d", state.Counters.HostsCreated), OwnerUID: ownerUID, Slot: slot}
		state.Hosts = append(state.Hosts, host)
		state.event("host-created", ownerUID, host.ID, "")
		return ensureResult{host.allocation(), state.loseResponse(ownerUID, "allocation")}, nil
	})
}

func (s *Server) ensureImage(ctx context.Context, req lab.ImageRequest) (any, error) {
	return s.change(ctx, func(state *diskState) (any, error) {
		if req.OwnerUID == "" || req.OperationID == "" || req.HostID == "" || req.ImageVersion == "" {
			return nil, lab.ErrInvalidImageRequest
		}
		if _, err := state.ownedHost(req.OwnerUID, req.HostID); err != nil {
			return nil, err
		}
		for _, job := range state.Jobs {
			if job.Request.OwnerUID == req.OwnerUID && job.Request.OperationID == req.OperationID {
				if job.Request != req {
					return nil, lab.ErrImageRequestConflict
				}
				return ensureResult{job.job(time.Now()), state.loseResponse(req.OwnerUID, "image")}, nil
			}
		}
		delay := s.imageDelay.Milliseconds()
		if global := state.Faults[""].ImageDelayMillis; global != nil {
			delay = *global
		}
		if owner := state.Faults[req.OwnerUID].ImageDelayMillis; owner != nil {
			delay = *owner
		}
		state.Counters.ImagesCreated++
		job := Image{ID: fmt.Sprintf("image-%03d", state.Counters.ImagesCreated), Request: req, StartedAt: time.Now().UTC(), DelayMillis: delay}
		state.Jobs = append(state.Jobs, job)
		state.event("image-created", req.OwnerUID, req.HostID, req.OperationID)
		return ensureResult{job.job(time.Now()), state.loseResponse(req.OwnerUID, "image")}, nil
	})
}

func (s *Server) lifecycle(ctx context.Context, kind string, req lab.HostOperation) (any, error) {
	return s.change(ctx, func(state *diskState) (any, error) {
		if req.OwnerUID == "" || req.OperationID == "" || req.HostID == "" || req.GPUClass == "" {
			return nil, ErrInvalidOperation
		}
		index, err := state.ownedHost(req.OwnerUID, req.HostID)
		if err != nil {
			return nil, err
		}
		for _, action := range state.Operations {
			if action.Kind == kind && action.Request.OwnerUID == req.OwnerUID && action.Request.OperationID == req.OperationID {
				if action.Request != req {
					return nil, ErrOperationConflict
				}
				return struct{}{}, nil
			}
		}
		if kind == "join" {
			imageReady := false
			for _, job := range state.Jobs {
				if job.Request.OwnerUID == req.OwnerUID && job.Request.HostID == req.HostID && job.job(time.Now()).Done {
					imageReady = true
					break
				}
			}
			if !imageReady {
				return nil, ErrImageNotReady
			}
			if !state.Hosts[index].Joined {
				state.Hosts[index].Joined = true
				state.Counters.Joins++
				state.event("host-joined", req.OwnerUID, req.HostID, req.OperationID)
			}
		} else {
			if !state.Hosts[index].Joined {
				return nil, ErrHostNotJoined
			}
			if req.GPUClass != "mock-a100" && req.GPUClass != "mock-h100" {
				return nil, lab.ErrUnsupportedGPUClass
			}
			if state.Faults[req.OwnerUID].FailValidation || state.Faults[""].FailValidation {
				return nil, fmt.Errorf("injected terminal validation failure: %w", lab.ErrValidationFailed)
			}
			if !state.Hosts[index].Validated {
				state.Hosts[index].Validated = true
				state.Counters.Validations++
				state.event("gpu-validated", req.OwnerUID, req.HostID, req.OperationID)
			}
		}
		state.Operations = append(state.Operations, operation{Kind: kind, Request: req})
		return struct{}{}, nil
	})
}

func (s *Server) release(ctx context.Context, ownerUID string) (any, error) {
	return s.change(ctx, func(state *diskState) (any, error) {
		if ownerUID == "" {
			return nil, lab.ErrInvalidDesiredHosts
		}
		if !state.ClosedOwners[ownerUID] {
			state.ClosedOwners[ownerUID] = true
			remaining := state.Hosts[:0]
			for _, host := range state.Hosts {
				if host.OwnerUID != ownerUID {
					remaining = append(remaining, host)
				}
			}
			state.Hosts = remaining
			state.Counters.Releases++
			state.event("owner-released", ownerUID, "", "")
		}
		return struct{}{}, nil
	})
}

func (s *Server) snapshot() Inventory {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := Inventory{Hosts: append([]Host{}, s.state.Hosts...), Jobs: append([]Image{}, s.state.Jobs...), ClosedOwners: []string{}, Counters: s.state.Counters, Events: append([]Event{}, s.state.Events...), Faults: make(map[string]faultConfig, len(s.state.Faults))}
	for ownerUID, closed := range s.state.ClosedOwners {
		if closed {
			result.ClosedOwners = append(result.ClosedOwners, ownerUID)
		}
	}
	for ownerUID, faults := range s.state.Faults {
		result.Faults[ownerUID] = faults
	}
	for i := range result.Jobs {
		result.Jobs[i].Done = result.Jobs[i].job(time.Now()).Done
	}
	sort.Slice(result.Hosts, func(i, j int) bool {
		if result.Hosts[i].OwnerUID != result.Hosts[j].OwnerUID {
			return result.Hosts[i].OwnerUID < result.Hosts[j].OwnerUID
		}
		return result.Hosts[i].Slot < result.Hosts[j].Slot
	})
	sort.Strings(result.ClosedOwners)
	return result
}

// Handler returns the provider API and local-only inspection/fault controls.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /v1/inventory", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, s.snapshot())
	})
	mux.HandleFunc("GET /v1/hosts", func(w http.ResponseWriter, r *http.Request) {
		hosts := []lab.HostAllocation{}
		for _, host := range s.snapshot().Hosts {
			if host.OwnerUID == r.URL.Query().Get("ownerUID") {
				hosts = append(hosts, host.allocation())
			}
		}
		writeJSON(w, http.StatusOK, hosts)
	})
	mux.HandleFunc("POST /v1/hosts/ensure", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			OwnerUID string `json:"ownerUID"`
			Slot     int    `json:"slot"`
		}
		if !readJSON(w, r, &request) {
			return
		}
		result, err := s.ensureHost(r.Context(), request.OwnerUID, request.Slot)
		finishEnsure(w, result, err)
	})
	mux.HandleFunc("POST /v1/images/ensure", func(w http.ResponseWriter, r *http.Request) {
		var request lab.ImageRequest
		if !readJSON(w, r, &request) {
			return
		}
		result, err := s.ensureImage(r.Context(), request)
		finishEnsure(w, result, err)
	})
	mux.HandleFunc("GET /v1/images/{jobID}", func(w http.ResponseWriter, r *http.Request) {
		ownerUID, jobID := r.URL.Query().Get("ownerUID"), r.PathValue("jobID")
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.state.ClosedOwners[ownerUID] {
			writeError(w, lab.ErrOwnerReleased)
			return
		}
		for _, job := range s.state.Jobs {
			if job.Request.OwnerUID == ownerUID && job.ID == jobID {
				writeJSON(w, http.StatusOK, job.job(time.Now()))
				return
			}
		}
		writeError(w, lab.ErrImageJobNotFound)
	})
	mux.HandleFunc("POST /v1/hosts/release", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			OwnerUID string `json:"ownerUID"`
		}
		if !readJSON(w, r, &request) {
			return
		}
		result, err := s.release(r.Context(), request.OwnerUID)
		finish(w, result, err)
	})
	for _, kind := range []string{"join", "validate"} {
		mux.HandleFunc("POST /v1/hosts/"+kind, func(w http.ResponseWriter, r *http.Request) {
			var request lab.HostOperation
			if !readJSON(w, r, &request) {
				return
			}
			result, err := s.lifecycle(r.Context(), kind, request)
			finish(w, result, err)
		})
	}
	mux.HandleFunc("POST /v1/faults", func(w http.ResponseWriter, r *http.Request) {
		var request FaultRequest
		if !readJSON(w, r, &request) {
			return
		}
		if request.ImageDelayMillis != nil && (*request.ImageDelayMillis < 0 || *request.ImageDelayMillis > int64((24*time.Hour)/time.Millisecond)) {
			writeJSON(w, http.StatusBadRequest, wireError{Code: "invalid_request", Message: "image delay must be between 0 and 86400000 milliseconds"})
			return
		}
		result, err := s.change(r.Context(), func(state *diskState) (any, error) {
			faults := state.Faults[request.OwnerUID]
			if request.LoseAllocationResponseOnce != nil {
				faults.LoseAllocationResponseOnce = *request.LoseAllocationResponseOnce
			}
			if request.LoseImageResponseOnce != nil {
				faults.LoseImageResponseOnce = *request.LoseImageResponseOnce
			}
			if request.FailValidation != nil {
				faults.FailValidation = *request.FailValidation
			}
			if request.ImageDelayMillis != nil {
				faults.ImageDelayMillis = request.ImageDelayMillis
			}
			state.Faults[request.OwnerUID] = faults
			state.event("faults-updated", request.OwnerUID, "", "")
			return faults, nil
		})
		finish(w, result, err)
	})
	return mux
}

func readJSON(w http.ResponseWriter, r *http.Request, request any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	err := decoder.Decode(request)
	if err == nil {
		var trailing any
		if trailingErr := decoder.Decode(&trailing); trailingErr != io.EOF {
			if trailingErr != nil {
				err = trailingErr
			} else {
				err = errors.New("request must contain exactly one JSON value")
			}
		}
	}
	if err != nil {
		status := http.StatusBadRequest
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		writeJSON(w, status, wireError{Code: "invalid_request", Message: err.Error()})
		return false
	}
	return true
}

func finishEnsure(w http.ResponseWriter, result any, err error) {
	if err != nil {
		writeError(w, err)
		return
	}
	ensured := result.(ensureResult)
	if ensured.lost {
		connection, _, err := http.NewResponseController(w).Hijack()
		if err == nil {
			connection.Close()
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, wireError{Code: "response_lost", Message: "simulated response lost after commit"})
		return
	}
	writeJSON(w, http.StatusOK, ensured.value)
}

func finish(w http.ResponseWriter, result any, err error) {
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}

type wireError struct {
	Code    string `json:"code"`
	Message string `json:"error"`
}

var errorClasses = []struct {
	err    error
	code   string
	status int
}{
	{lab.ErrInvalidAllocation, "invalid_allocation", http.StatusBadRequest},
	{lab.ErrInvalidDesiredHosts, "invalid_desired_hosts", http.StatusBadRequest},
	{lab.ErrInvalidImageRequest, "invalid_image_request", http.StatusBadRequest},
	{lab.ErrImageRequestConflict, "image_request_conflict", http.StatusConflict},
	{lab.ErrOwnerReleased, "owner_released", http.StatusGone},
	{lab.ErrHostNotOwned, "host_not_owned", http.StatusForbidden},
	{lab.ErrImageJobNotFound, "image_job_not_found", http.StatusNotFound},
	{lab.ErrUnsupportedGPUClass, "unsupported_gpu_class", http.StatusUnprocessableEntity},
	{lab.ErrValidationFailed, "validation_failed", http.StatusUnprocessableEntity},
	{ErrInvalidOperation, "invalid_operation", http.StatusBadRequest},
	{ErrOperationConflict, "operation_conflict", http.StatusConflict},
	{ErrImageNotReady, "image_not_ready", http.StatusConflict},
	{ErrHostNotJoined, "host_not_joined", http.StatusConflict},
}

func writeError(w http.ResponseWriter, err error) {
	for _, class := range errorClasses {
		if errors.Is(err, class.err) {
			writeJSON(w, class.status, wireError{Code: class.code, Message: err.Error()})
			return
		}
	}
	writeJSON(w, http.StatusInternalServerError, wireError{Code: "provider_error", Message: err.Error()})
}
