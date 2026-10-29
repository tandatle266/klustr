package kube

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	pfReadyTimeout = 10 * time.Second
	pfMaxBackoff   = 30 * time.Second
)

// Port-forward statuses as the frontend sees them.
const (
	pfStopped      = "stopped"      // turned off by the user
	pfIdle         = "idle"         // on, waiting for its context to connect
	pfStarting     = "starting"     // first connection in progress
	pfActive       = "active"       // listening locally
	pfReconnecting = "reconnecting" // lost its pod, retrying with backoff
	pfError        = "error"        // gave up; Start tries again
)

var errPFNotFound = errors.New("port-forward not found")

// PortForwardSpec is what a saved forward is: the target and how to reach it.
// It is the persisted shape, so it carries nothing about the running state.
type PortForwardSpec struct {
	ID        string `json:"id"`
	Context   string `json:"context"`
	Namespace string `json:"namespace"`
	Kind      string `json:"kind"` // Pod or Service
	Name      string `json:"name"`
	// Port is the container port of a Pod or the port of a Service.
	Port uint16 `json:"port"`
	// LocalPort is the requested local port; 0 lets the OS pick one.
	LocalPort uint16 `json:"localPort"`
	Protocol  string `json:"protocol"` // http or https, for the browser link
	Enabled   bool   `json:"enabled"`
}

type PortForwardInfo struct {
	ID         string `json:"id"`
	Context    string `json:"context"`
	Namespace  string `json:"namespace"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	Port       uint16 `json:"port"`
	LocalPort  uint16 `json:"localPort"`
	Protocol   string `json:"protocol"`
	Enabled    bool   `json:"enabled"`
	Status     string `json:"status"`
	BoundPort  uint16 `json:"boundPort"`  // the port listening locally while active
	PodName    string `json:"podName"`    // the pod the forward reaches
	TargetPort uint16 `json:"targetPort"` // the container port on that pod
	Error      string `json:"error"`
}

// pfAttempt is one connection of a forward to a resolved pod. lost, when not
// nil, closes once that pod is gone, so the forward reconnects instead of
// failing every new connection against a dead pod.
type pfAttempt struct {
	forwarder portForwarder
	pod       string
	port      uint16
	lost      <-chan struct{}
}

type pfConnect func(ctx context.Context, spec PortForwardSpec, localPort uint16, ready chan struct{}) (pfAttempt, error)

type pfChangeFunc func()

// pfRun is one supervisor goroutine. An entry points at its current run, and
// a supervisor only writes the entry while it is still that run, so a stopped
// or restarted forward never gets overwritten by the run it replaced.
type pfRun struct {
	cancel context.CancelFunc
}

type pfEntry struct {
	spec   PortForwardSpec
	status string
	bound  uint16
	pod    string
	target uint16
	err    string
	run    *pfRun
}

func (e *pfEntry) info() PortForwardInfo {
	return PortForwardInfo{
		ID:         e.spec.ID,
		Context:    e.spec.Context,
		Namespace:  e.spec.Namespace,
		Kind:       e.spec.Kind,
		Name:       e.spec.Name,
		Port:       e.spec.Port,
		LocalPort:  e.spec.LocalPort,
		Protocol:   e.spec.Protocol,
		Enabled:    e.spec.Enabled,
		Status:     e.status,
		BoundPort:  e.bound,
		PodName:    e.pod,
		TargetPort: e.target,
		Error:      e.err,
	}
}

type pfManager struct {
	mu       sync.Mutex
	entries  map[string]*pfEntry
	path     string
	connect  pfConnect
	backoff  func(failures int) time.Duration
	onChange pfChangeFunc
}

// newPFManager loads the saved forwards from path ("" keeps them in memory
// only). Enabled ones wait as idle until resume starts their context.
func newPFManager(path string, connect pfConnect) *pfManager {
	mgr := &pfManager{
		entries: make(map[string]*pfEntry),
		path:    path,
		connect: connect,
		backoff: pfBackoff,
	}
	if path == "" {
		return mgr
	}
	specs, err := loadPortForwards(path)
	if err != nil {
		return mgr
	}
	for _, spec := range specs {
		status := pfStopped
		if spec.Enabled {
			status = pfIdle
		}
		mgr.entries[spec.ID] = &pfEntry{spec: spec, status: status}
	}
	return mgr
}

func pfBackoff(failures int) time.Duration {
	d := time.Second << min(failures-1, 5)
	return min(d, pfMaxBackoff)
}

func newPFID() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return "pf-" + hex.EncodeToString(b)
}

func normalizePFProtocol(p string) string {
	if p == "https" {
		return p
	}
	return "http"
}

func (mgr *pfManager) setOnChange(cb pfChangeFunc) {
	mgr.mu.Lock()
	mgr.onChange = cb
	mgr.mu.Unlock()
}

func (mgr *pfManager) notify() {
	mgr.mu.Lock()
	cb := mgr.onChange
	mgr.mu.Unlock()
	if cb != nil {
		cb()
	}
}

// saveLocked persists the specs. A failed write only costs the restore on the
// next launch, so it does not fail the action that triggered it.
func (mgr *pfManager) saveLocked() {
	if mgr.path == "" {
		return
	}
	specs := make([]PortForwardSpec, 0, len(mgr.entries))
	for _, e := range mgr.entries {
		specs = append(specs, e.spec)
	}
	sortPFSpecs(specs)
	_ = savePortForwards(mgr.path, specs)
}

func sortPFSpecs(specs []PortForwardSpec) {
	sort.Slice(specs, func(i, j int) bool {
		a, b := specs[i], specs[j]
		if a.Context != b.Context {
			return a.Context < b.Context
		}
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		if a.Port != b.Port {
			return a.Port < b.Port
		}
		return a.ID < b.ID
	})
}

// add saves a forward and starts it, waiting for the first connection. A
// forward to the same port of the same resource is updated instead of
// duplicated.
func (mgr *pfManager) add(spec PortForwardSpec) (PortForwardInfo, error) {
	if spec.Kind != "Pod" && spec.Kind != "Service" {
		return PortForwardInfo{}, fmt.Errorf("cannot port-forward a %s", spec.Kind)
	}
	if spec.Context == "" || spec.Namespace == "" || spec.Name == "" || spec.Port == 0 {
		return PortForwardInfo{}, errors.New("port-forward needs a context, namespace, name and port")
	}
	spec.Protocol = normalizePFProtocol(spec.Protocol)
	mgr.mu.Lock()
	var entry *pfEntry
	for _, e := range mgr.entries {
		s := e.spec
		if s.Context == spec.Context && s.Namespace == spec.Namespace && s.Kind == spec.Kind && s.Name == spec.Name && s.Port == spec.Port {
			entry = e
			break
		}
	}
	if entry == nil {
		spec.ID = newPFID()
		spec.Enabled = false
		entry = &pfEntry{spec: spec, status: pfStopped}
		mgr.entries[spec.ID] = entry
	} else {
		entry.spec.LocalPort = spec.LocalPort
		entry.spec.Protocol = spec.Protocol
	}
	id := entry.spec.ID
	mgr.mu.Unlock()
	return mgr.start(id)
}

// start turns a forward on (restarting it if it runs) and waits for the first
// connection. A failure turns it back off with the error shown, rather than
// retrying something the user is watching fail.
func (mgr *pfManager) start(id string) (PortForwardInfo, error) {
	mgr.mu.Lock()
	e, ok := mgr.entries[id]
	if !ok {
		mgr.mu.Unlock()
		return PortForwardInfo{}, errPFNotFound
	}
	e.spec.Enabled = true
	ctx, run := mgr.launchLocked(e)
	mgr.saveLocked()
	mgr.mu.Unlock()
	mgr.notify()

	first := make(chan error, 1)
	go mgr.supervise(ctx, e, run, first)
	if err := <-first; err != nil {
		return PortForwardInfo{}, err
	}
	return mgr.get(id)
}

func (mgr *pfManager) launchLocked(e *pfEntry) (context.Context, *pfRun) {
	if e.run != nil {
		e.run.cancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	run := &pfRun{cancel: cancel}
	e.run = run
	e.status = pfStarting
	e.bound, e.pod, e.target, e.err = 0, "", 0, ""
	return ctx, run
}

// resume starts the enabled forwards of a context that just connected. A
// forward that is already running is left alone, so a re-watch after a
// credential refresh does not drop its connections.
func (mgr *pfManager) resume(contextName string) {
	type launch struct {
		ctx   context.Context
		entry *pfEntry
		run   *pfRun
	}
	var launches []launch
	mgr.mu.Lock()
	for _, e := range mgr.entries {
		if e.spec.Context == contextName && e.spec.Enabled && e.run == nil {
			ctx, run := mgr.launchLocked(e)
			launches = append(launches, launch{ctx, e, run})
		}
	}
	mgr.mu.Unlock()
	if len(launches) == 0 {
		return
	}
	for _, l := range launches {
		go mgr.supervise(l.ctx, l.entry, l.run, nil)
	}
	mgr.notify()
}

// supervise keeps one forward connected until its run is canceled. first,
// when set, receives the outcome of the first connection; without it (a
// restore) a failing first connection is retried like a lost one.
func (mgr *pfManager) supervise(ctx context.Context, e *pfEntry, run *pfRun, first chan<- error) {
	defer run.cancel()
	mgr.mu.Lock()
	spec := e.spec
	mgr.mu.Unlock()
	local := spec.LocalPort
	failures := 0
	for {
		conn, wait, err := mgr.open(ctx, spec, local)
		if err == nil {
			failures = 0
			// Reconnects reuse the first port the OS picked, so a link
			// the user already opened keeps working.
			local = conn.bound
			if !mgr.update(e, run, func(e *pfEntry) {
				e.status = pfActive
				e.bound, e.pod, e.target, e.err = conn.bound, conn.pod, conn.port, ""
			}) {
				if first != nil {
					first <- context.Canceled
				}
				return
			}
			if first != nil {
				first <- nil
				first = nil
			}
			err = wait()
		}
		if ctx.Err() != nil {
			if first != nil {
				first <- context.Canceled
			}
			return
		}
		// A start the user is waiting on reports its failure instead of
		// retrying; so does a local port conflict, which a retry cannot fix.
		if first != nil || isPFListenError(err) {
			waited := first != nil
			mgr.update(e, run, func(e *pfEntry) {
				e.status = pfError
				e.bound, e.err = 0, err.Error()
				e.run = nil
				if waited {
					e.spec.Enabled = false
					mgr.saveLocked()
				}
			})
			if first != nil {
				first <- err
			}
			return
		}
		failures++
		if !mgr.update(e, run, func(e *pfEntry) {
			e.status = pfReconnecting
			e.bound, e.err = 0, err.Error()
		}) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(mgr.backoff(failures)):
		}
	}
}

func isPFListenError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "unable to listen")
}

type pfConnected struct {
	bound uint16
	pod   string
	port  uint16
}

// open makes one connection and returns once it listens locally. wait then
// blocks until that connection ends and always releases the local listener
// before returning, so the next attempt can bind the same port.
func (mgr *pfManager) open(ctx context.Context, spec PortForwardSpec, local uint16) (pfConnected, func() error, error) {
	actx, cancel := context.WithCancel(ctx)
	ready := make(chan struct{})
	att, err := mgr.connect(actx, spec, local, ready)
	if err != nil {
		cancel()
		return pfConnected{}, nil, err
	}
	if err := ctx.Err(); err != nil {
		cancel()
		return pfConnected{}, nil, err
	}
	resultCh := make(chan error, 1)
	go func() {
		resultCh <- att.forwarder.ForwardPorts()
	}()

	timeout := time.NewTimer(pfReadyTimeout)
	defer timeout.Stop()
	select {
	case <-ctx.Done():
		cancel()
		return pfConnected{}, nil, ctx.Err()
	case <-ready:
	case err := <-resultCh:
		cancel()
		if err == nil {
			err = errors.New("port-forward terminated before ready")
		}
		return pfConnected{}, nil, err
	case <-timeout.C:
		cancel()
		return pfConnected{}, nil, errors.New("timed out waiting for port-forward to become ready")
	}

	ports, err := att.forwarder.GetPorts()
	if err == nil && len(ports) == 0 {
		err = errors.New("no ports reported by forwarder")
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		cancel()
		return pfConnected{}, nil, err
	}

	wait := func() error {
		defer cancel()
		select {
		case err := <-resultCh:
			if err == nil {
				err = errors.New("port-forward closed")
			}
			return err
		case <-att.lost:
			cancel()
			<-resultCh //nolint:errcheck // the lost pod is the error worth showing
			return fmt.Errorf("pod %s is gone or no longer running", att.pod)
		case <-ctx.Done():
			cancel()
			<-resultCh //nolint:errcheck // a canceled run has nothing to report
			return ctx.Err()
		}
	}
	return pfConnected{bound: ports[0].Local, pod: att.pod, port: att.port}, wait, nil
}

// update applies fn while run is still the entry's current run and reports
// whether it was.
func (mgr *pfManager) update(e *pfEntry, run *pfRun, fn func(*pfEntry)) bool {
	mgr.mu.Lock()
	if mgr.entries[e.spec.ID] != e || e.run != run {
		mgr.mu.Unlock()
		return false
	}
	fn(e)
	mgr.mu.Unlock()
	mgr.notify()
	return true
}

func (mgr *pfManager) get(id string) (PortForwardInfo, error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	e, ok := mgr.entries[id]
	if !ok {
		return PortForwardInfo{}, errPFNotFound
	}
	return e.info(), nil
}

// stop turns a forward off and keeps it listed so it can be started again.
func (mgr *pfManager) stop(id string) error {
	mgr.mu.Lock()
	e, ok := mgr.entries[id]
	if !ok {
		mgr.mu.Unlock()
		return errPFNotFound
	}
	if e.run != nil {
		e.run.cancel()
		e.run = nil
	}
	e.spec.Enabled = false
	e.status = pfStopped
	e.bound, e.pod, e.target, e.err = 0, "", 0, ""
	mgr.saveLocked()
	mgr.mu.Unlock()
	mgr.notify()
	return nil
}

// edit changes the local port and protocol, restarting a running forward so
// the new port takes effect.
func (mgr *pfManager) edit(id string, localPort uint16, protocol string) (PortForwardInfo, error) {
	mgr.mu.Lock()
	e, ok := mgr.entries[id]
	if !ok {
		mgr.mu.Unlock()
		return PortForwardInfo{}, errPFNotFound
	}
	restart := e.run != nil && e.spec.LocalPort != localPort
	e.spec.LocalPort = localPort
	e.spec.Protocol = normalizePFProtocol(protocol)
	mgr.saveLocked()
	info := e.info()
	mgr.mu.Unlock()
	if restart {
		return mgr.start(id)
	}
	mgr.notify()
	return info, nil
}

func (mgr *pfManager) remove(id string) error {
	mgr.mu.Lock()
	e, ok := mgr.entries[id]
	if !ok {
		mgr.mu.Unlock()
		return errPFNotFound
	}
	if e.run != nil {
		e.run.cancel()
	}
	delete(mgr.entries, id)
	mgr.saveLocked()
	mgr.mu.Unlock()
	mgr.notify()
	return nil
}

func (mgr *pfManager) list() []PortForwardInfo {
	mgr.mu.Lock()
	specs := make([]PortForwardSpec, 0, len(mgr.entries))
	for _, e := range mgr.entries {
		specs = append(specs, e.spec)
	}
	sortPFSpecs(specs)
	out := make([]PortForwardInfo, 0, len(specs))
	for _, s := range specs {
		out = append(out, mgr.entries[s.ID].info())
	}
	mgr.mu.Unlock()
	return out
}

// suspendContext stops the running forwards of a context on disconnect,
// releasing their local listeners. They stay enabled, so resume restarts
// them on the next connect.
func (mgr *pfManager) suspendContext(contextName string) {
	if mgr.suspend(func(e *pfEntry) bool { return e.spec.Context == contextName }) {
		mgr.notify()
	}
}

// stopAll releases every listener on app quit. Forwards stay enabled in the
// saved file, so the next launch restores them.
func (mgr *pfManager) stopAll() {
	mgr.suspend(func(*pfEntry) bool { return true })
}

func (mgr *pfManager) suspend(match func(*pfEntry) bool) bool {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	changed := false
	for _, e := range mgr.entries {
		if e.run == nil || !match(e) {
			continue
		}
		e.run.cancel()
		e.run = nil
		e.status = pfIdle
		e.bound, e.err = 0, ""
		changed = true
	}
	return changed
}
