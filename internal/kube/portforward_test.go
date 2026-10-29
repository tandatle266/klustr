package kube

import (
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gorilla/websocket"
	corev1 "k8s.io/api/core/v1"
	pfconstants "k8s.io/apimachinery/pkg/util/portforward"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/streaming/pkg/httpstream"
	httpstreamspdy "k8s.io/streaming/pkg/httpstream/spdy"
)

var testPFSpec = PortForwardSpec{Context: "ctx-a", Namespace: "default", Kind: "Pod", Name: "pod", Port: 80}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// newTestPFManager keeps forwards in memory and connects through build, which
// stands in for resolving the pod and dialing the API server.
func newTestPFManager(build func(ctx context.Context, ready chan struct{}) (portForwarder, error)) *pfManager {
	return newPFManager("", func(ctx context.Context, spec PortForwardSpec, _ uint16, ready chan struct{}) (pfAttempt, error) {
		fw, err := build(ctx, ready)
		if err != nil {
			return pfAttempt{}, err
		}
		return pfAttempt{forwarder: fw, pod: spec.Name, port: spec.Port}, nil
	})
}

func (mgr *pfManager) runningForTest(id string) bool {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	e, ok := mgr.entries[id]
	return ok && e.run != nil
}

// addRunningForTest registers an enabled, active forward whose run closes
// the returned channel when canceled.
func (mgr *pfManager) addRunningForTest(id, contextName string) <-chan struct{} {
	ctx, cancel := context.WithCancel(context.Background())
	spec := testPFSpec
	spec.ID, spec.Context, spec.Enabled = id, contextName, true
	mgr.mu.Lock()
	mgr.entries[id] = &pfEntry{spec: spec, status: pfActive, bound: 30800, run: &pfRun{cancel: cancel}}
	mgr.mu.Unlock()
	return ctx.Done()
}

type fakePortForwarder struct {
	run   func() error
	ports func() ([]portforward.ForwardedPort, error)
}

func (f *fakePortForwarder) ForwardPorts() error { return f.run() }
func (f *fakePortForwarder) GetPorts() ([]portforward.ForwardedPort, error) {
	return f.ports()
}

// readyForwarder becomes ready on port local and runs until ctx ends or
// finish delivers an error.
func readyForwarder(ctx context.Context, ready chan struct{}, local uint16, finish <-chan error) *fakePortForwarder {
	return &fakePortForwarder{
		run: func() error {
			close(ready)
			select {
			case err := <-finish:
				return err
			case <-ctx.Done():
				return nil
			}
		},
		ports: func() ([]portforward.ForwardedPort, error) {
			return []portforward.ForwardedPort{{Local: local, Remote: 80}}, nil
		},
	}
}

func receivePFTest[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for port-forward test")
		var zero T
		return zero
	}
}

func waitPFStatus(t *testing.T, mgr *pfManager, id, status string) PortForwardInfo {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		info, err := mgr.get(id)
		if err == nil && info.Status == status {
			return info
		}
		if time.Now().After(deadline) {
			t.Fatalf("forward %s never reached %s: %+v (err %v)", id, status, info, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestSuspendContextStopsOnlyMatchingForwardsAndKeepsThemEnabled(t *testing.T) {
	mgr := newPFManager("", nil)
	var notified int
	mgr.setOnChange(func() { notified++ })

	a1 := mgr.addRunningForTest("pf-1", "ctx-a")
	a2 := mgr.addRunningForTest("pf-2", "ctx-a")
	b1 := mgr.addRunningForTest("pf-3", "ctx-b")

	mgr.suspendContext("ctx-a")

	if !isClosed(a1) || !isClosed(a2) {
		t.Fatalf("expected ctx-a forwards to be canceled")
	}
	if isClosed(b1) {
		t.Fatalf("expected ctx-b forward to keep running")
	}
	for _, info := range mgr.list() {
		want := pfIdle
		if info.Context == "ctx-b" {
			want = pfActive
		}
		if info.Status != want || !info.Enabled {
			t.Fatalf("forward %s = %s enabled=%v, want %s enabled", info.ID, info.Status, info.Enabled, want)
		}
	}
	if notified != 1 {
		t.Fatalf("expected exactly one change notification, got %d", notified)
	}
}

func TestSuspendContextNoMatchDoesNotNotify(t *testing.T) {
	mgr := newPFManager("", nil)
	var notified int
	mgr.setOnChange(func() { notified++ })
	mgr.addRunningForTest("pf-1", "ctx-a")

	mgr.suspendContext("ctx-missing")

	if notified != 0 {
		t.Fatalf("expected no notification when nothing matched, got %d", notified)
	}
	if !mgr.runningForTest("pf-1") {
		t.Fatal("unmatched forward was stopped")
	}
}

func TestPortForwardStopCancelsPendingSetup(t *testing.T) {
	for _, stopAll := range []bool{false, true} {
		t.Run(map[bool]string{false: "disconnect", true: "shutdown"}[stopAll], func(t *testing.T) {
			entered := make(chan struct{})
			mgr := newTestPFManager(func(ctx context.Context, _ chan struct{}) (portForwarder, error) {
				close(entered)
				<-ctx.Done()
				return nil, ctx.Err()
			})
			t.Cleanup(mgr.stopAll)
			other := mgr.addRunningForTest("other", "ctx-b")
			result := make(chan error, 1)
			go func() {
				_, err := mgr.add(testPFSpec)
				result <- err
			}()
			receivePFTest(t, entered)
			if stopAll {
				mgr.stopAll()
			} else {
				mgr.suspendContext("ctx-a")
			}
			if err := receivePFTest(t, result); !errors.Is(err, context.Canceled) {
				t.Fatalf("start error = %v, want cancellation", err)
			}
			if isClosed(other) != stopAll {
				t.Fatal("unexpected cancellation of the other context")
			}
		})
	}
}

func TestPortForwardDisconnectRejectsLateReadiness(t *testing.T) {
	readingPorts := make(chan struct{})
	release := make(chan struct{})
	exited := make(chan struct{})
	mgr := newTestPFManager(func(ctx context.Context, ready chan struct{}) (portForwarder, error) {
		return &fakePortForwarder{
			run: func() error {
				defer close(exited)
				close(ready)
				<-ctx.Done()
				return nil
			},
			ports: func() ([]portforward.ForwardedPort, error) {
				close(readingPorts)
				<-release
				return []portforward.ForwardedPort{{Local: 30800, Remote: 80}}, nil
			},
		}, nil
	})
	t.Cleanup(mgr.stopAll)
	result := make(chan error, 1)
	go func() {
		_, err := mgr.add(testPFSpec)
		result <- err
	}()
	receivePFTest(t, readingPorts)
	mgr.suspendContext("ctx-a")
	close(release)
	if err := receivePFTest(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("late start error = %v, want cancellation", err)
	}
	receivePFTest(t, exited)
	got := mgr.list()
	if len(got) != 1 || got[0].Status != pfIdle || mgr.runningForTest(got[0].ID) {
		t.Fatalf("late readiness resurrected the canceled forward: %+v", got)
	}
}

type pfTestTransport func(*http.Request) (*http.Response, error)

func (f pfTestTransport) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestPortForwardDisconnectCancelsHandshake(t *testing.T) {
	entered := make(chan struct{})
	exited := make(chan struct{})
	cfg := &rest.Config{
		Host: "https://portforward.example.invalid",
		WrapTransport: func(http.RoundTripper) http.RoundTripper {
			return pfTestTransport(func(req *http.Request) (*http.Response, error) {
				defer close(exited)
				close(entered)
				<-req.Context().Done()
				return nil, req.Context().Err()
			})
		},
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	mgr := newTestPFManager(func(ctx context.Context, ready chan struct{}) (portForwarder, error) {
		return newPortForwarder(ctx, cs, cfg, "default", "pod", 0, 80, ready)
	})
	t.Cleanup(mgr.stopAll)
	result := make(chan error, 1)
	go func() {
		_, err := mgr.add(testPFSpec)
		result <- err
	}()
	receivePFTest(t, entered)
	mgr.suspendContext("ctx-a")
	if err := receivePFTest(t, result); err == nil {
		t.Fatal("canceled handshake reported success")
	}
	receivePFTest(t, exited)
	if got := mgr.list(); len(got) != 1 || mgr.runningForTest(got[0].ID) {
		t.Fatalf("canceled handshake kept running: %+v", got)
	}
}

func TestPortForwardReconnectsOnTheSameLocalPort(t *testing.T) {
	finish := make(chan error, 1)
	var locals []uint16
	attempts := make(chan struct{}, 4)
	mgr := newPFManager("", func(ctx context.Context, spec PortForwardSpec, local uint16, ready chan struct{}) (pfAttempt, error) {
		locals = append(locals, local)
		attempts <- struct{}{}
		return pfAttempt{forwarder: readyForwarder(ctx, ready, 30800, finish), pod: "pod", port: spec.Port}, nil
	})
	mgr.backoff = func(int) time.Duration { return time.Millisecond }
	t.Cleanup(mgr.stopAll)

	info, err := mgr.add(testPFSpec)
	if err != nil {
		t.Fatal(err)
	}
	if info.BoundPort != 30800 || info.TargetPort != 80 || info.PodName != "pod" || info.Status != pfActive || !info.Enabled {
		t.Fatalf("unexpected forward: %+v", info)
	}
	receivePFTest(t, attempts)
	finish <- errors.New("connection lost")
	receivePFTest(t, attempts)
	waitPFStatus(t, mgr, info.ID, pfActive)
	if !slices.Equal(locals, []uint16{0, 30800}) {
		t.Fatalf("local ports per attempt = %v, want the picked port reused", locals)
	}
}

func TestPortForwardReconnectsWhenThePodIsLost(t *testing.T) {
	lost := make(chan struct{})
	canceled := make(chan struct{}, 1)
	attempts := 0
	mgr := newPFManager("", func(ctx context.Context, spec PortForwardSpec, _ uint16, ready chan struct{}) (pfAttempt, error) {
		attempts++
		if attempts == 1 {
			go func() {
				<-ctx.Done()
				canceled <- struct{}{}
			}()
			return pfAttempt{forwarder: readyForwarder(ctx, ready, 30800, nil), pod: "pod-old", port: 80, lost: lost}, nil
		}
		return pfAttempt{forwarder: readyForwarder(ctx, ready, 30800, nil), pod: "pod-new", port: 80}, nil
	})
	mgr.backoff = func(int) time.Duration { return time.Millisecond }
	t.Cleanup(mgr.stopAll)
	info, err := mgr.add(PortForwardSpec{Context: "ctx-a", Namespace: "default", Kind: "Service", Name: "web", Port: 80})
	if err != nil {
		t.Fatal(err)
	}
	close(lost)
	receivePFTest(t, canceled)
	deadline := time.Now().Add(3 * time.Second)
	for {
		got, _ := mgr.get(info.ID)
		if got.PodName == "pod-new" && got.Status == pfActive {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("forward did not move to the new pod: %+v", got)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestPortForwardLocalPortConflictIsNotRetried(t *testing.T) {
	var attempts atomic.Int32
	mgr := newTestPFManager(func(ctx context.Context, _ chan struct{}) (portForwarder, error) {
		attempts.Add(1)
		return &fakePortForwarder{run: func() error {
			return errors.New("unable to listen on any of the requested ports: [{8080 80}]")
		}}, nil
	})
	mgr.backoff = func(int) time.Duration { return time.Millisecond }
	t.Cleanup(mgr.stopAll)
	spec := testPFSpec
	spec.ID, spec.Enabled = "pf-1", true
	mgr.entries["pf-1"] = &pfEntry{spec: spec, status: pfIdle}

	mgr.resume("ctx-a")
	info := waitPFStatus(t, mgr, "pf-1", pfError)
	if !strings.Contains(info.Error, "unable to listen") || !info.Enabled {
		t.Fatalf("conflict was not reported on the enabled forward: %+v", info)
	}
	time.Sleep(20 * time.Millisecond)
	if n := attempts.Load(); n != 1 {
		t.Fatalf("port conflict was retried: %d attempts", n)
	}
}

func TestPortForwardRestoreRetriesAFailingFirstConnection(t *testing.T) {
	var attempts atomic.Int32
	mgr := newTestPFManager(func(ctx context.Context, ready chan struct{}) (portForwarder, error) {
		if attempts.Add(1) == 1 {
			return nil, errors.New("apiserver unreachable")
		}
		return readyForwarder(ctx, ready, 30800, nil), nil
	})
	mgr.backoff = func(int) time.Duration { return time.Millisecond }
	t.Cleanup(mgr.stopAll)
	spec := testPFSpec
	spec.ID, spec.Enabled = "pf-1", true
	mgr.entries["pf-1"] = &pfEntry{spec: spec, status: pfIdle}

	mgr.resume("ctx-a")
	waitPFStatus(t, mgr, "pf-1", pfActive)
	mgr.resume("ctx-a")
	if n := attempts.Load(); n != 2 {
		t.Fatalf("attempts = %d, want a retry and no second supervisor", n)
	}
}

func TestPortForwardFailedStartLeavesTheForwardStopped(t *testing.T) {
	for _, stage := range []string{"build", "connect", "ports"} {
		t.Run(stage, func(t *testing.T) {
			wantErr := errors.New("start failed")
			var sessionCtx context.Context
			mgr := newTestPFManager(func(ctx context.Context, ready chan struct{}) (portForwarder, error) {
				sessionCtx = ctx
				if stage == "build" {
					return nil, wantErr
				}
				return &fakePortForwarder{
					run: func() error {
						if stage == "connect" {
							return wantErr
						}
						close(ready)
						<-ctx.Done()
						return nil
					},
					ports: func() ([]portforward.ForwardedPort, error) { return nil, wantErr },
				}, nil
			})
			t.Cleanup(mgr.stopAll)
			_, err := mgr.add(testPFSpec)
			if !errors.Is(err, wantErr) {
				t.Fatalf("start error = %v, want %v", err, wantErr)
			}
			if !errors.Is(sessionCtx.Err(), context.Canceled) {
				t.Fatal("failed start did not cancel the attempt")
			}
			got := mgr.list()
			if len(got) != 1 || got[0].Status != pfError || got[0].Enabled || got[0].Error != wantErr.Error() || mgr.runningForTest(got[0].ID) {
				t.Fatalf("failed start = %+v, want a stopped forward showing the error", got)
			}
		})
	}
}

func TestPortForwardAddUpdatesTheSameTarget(t *testing.T) {
	mgr := newTestPFManager(func(ctx context.Context, ready chan struct{}) (portForwarder, error) {
		return readyForwarder(ctx, ready, 30800, nil), nil
	})
	t.Cleanup(mgr.stopAll)
	first, err := mgr.add(testPFSpec)
	if err != nil {
		t.Fatal(err)
	}
	again := testPFSpec
	again.LocalPort, again.Protocol = 9090, "https"
	second, err := mgr.add(again)
	if err != nil {
		t.Fatal(err)
	}
	if second.ID != first.ID || second.LocalPort != 9090 || second.Protocol != "https" || len(mgr.list()) != 1 {
		t.Fatalf("same target was duplicated or not updated: %+v", mgr.list())
	}
	other := testPFSpec
	other.Port = 81
	if _, err := mgr.add(other); err != nil {
		t.Fatal(err)
	}
	if len(mgr.list()) != 2 {
		t.Fatalf("another port of the pod should be its own forward: %+v", mgr.list())
	}
}

func TestPortForwardStopKeepsTheForwardAndRemoveDeletesIt(t *testing.T) {
	mgr := newTestPFManager(func(ctx context.Context, ready chan struct{}) (portForwarder, error) {
		return readyForwarder(ctx, ready, 30800, nil), nil
	})
	t.Cleanup(mgr.stopAll)
	info, err := mgr.add(testPFSpec)
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.stop(info.ID); err != nil {
		t.Fatal(err)
	}
	got, _ := mgr.get(info.ID)
	if got.Status != pfStopped || got.Enabled || got.BoundPort != 0 || mgr.runningForTest(info.ID) {
		t.Fatalf("stopped forward = %+v", got)
	}
	if got, err := mgr.start(info.ID); err != nil || got.Status != pfActive {
		t.Fatalf("restart = %+v, %v", got, err)
	}
	if err := mgr.remove(info.ID); err != nil {
		t.Fatal(err)
	}
	if len(mgr.list()) != 0 || mgr.runningForTest(info.ID) {
		t.Fatal("removed forward is still listed or running")
	}
	if err := mgr.stop(info.ID); !errors.Is(err, errPFNotFound) {
		t.Fatalf("stop of a removed forward = %v", err)
	}
}

func TestPortForwardEditRestartsOnANewLocalPort(t *testing.T) {
	var locals []uint16
	mgr := newPFManager("", func(ctx context.Context, spec PortForwardSpec, local uint16, ready chan struct{}) (pfAttempt, error) {
		locals = append(locals, local)
		port := local
		if port == 0 {
			port = 30800
		}
		return pfAttempt{forwarder: readyForwarder(ctx, ready, port, nil), pod: "pod", port: spec.Port}, nil
	})
	t.Cleanup(mgr.stopAll)
	info, err := mgr.add(testPFSpec)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := mgr.edit(info.ID, info.LocalPort, "https"); err != nil || got.Protocol != "https" || len(locals) != 1 {
		t.Fatalf("protocol-only edit = %+v, %v (attempts %v)", got, err, locals)
	}
	got, err := mgr.edit(info.ID, 9090, "https")
	if err != nil || got.BoundPort != 9090 || !slices.Equal(locals, []uint16{0, 9090}) {
		t.Fatalf("port edit = %+v, %v (attempts %v)", got, err, locals)
	}
}

func TestPortForwardsPersistAcrossRestarts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "klustr", "port-forwards.json")
	connect := func(ctx context.Context, spec PortForwardSpec, _ uint16, ready chan struct{}) (pfAttempt, error) {
		return pfAttempt{forwarder: readyForwarder(ctx, ready, 30800, nil), pod: spec.Name, port: spec.Port}, nil
	}
	mgr := newPFManager(path, connect)
	on, err := mgr.add(testPFSpec)
	if err != nil {
		t.Fatal(err)
	}
	offSpec := testPFSpec
	offSpec.Kind, offSpec.Name = "Service", "web"
	off, err := mgr.add(offSpec)
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.stop(off.ID); err != nil {
		t.Fatal(err)
	}
	mgr.stopAll()

	restored := newPFManager(path, connect)
	t.Cleanup(restored.stopAll)
	if got, _ := restored.get(on.ID); got.Status != pfIdle || !got.Enabled || got.Name != "pod" {
		t.Fatalf("enabled forward restored as %+v", got)
	}
	if got, _ := restored.get(off.ID); got.Status != pfStopped || got.Enabled {
		t.Fatalf("stopped forward restored as %+v", got)
	}
	restored.resume("ctx-other")
	if restored.runningForTest(on.ID) {
		t.Fatal("another context's connect started the forward")
	}
	restored.resume("ctx-a")
	waitPFStatus(t, restored, on.ID, pfActive)
	if restored.runningForTest(off.ID) {
		t.Fatal("a stopped forward was resumed")
	}
}

func TestPFBackoffIsCapped(t *testing.T) {
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, pfMaxBackoff, pfMaxBackoff}
	for i, w := range want {
		if got := pfBackoff(i + 1); got != w {
			t.Errorf("pfBackoff(%d) = %v, want %v", i+1, got, w)
		}
	}
}

func TestPortForwardCancelClosesBlockedUpgrade(t *testing.T) {
	for _, stage := range []string{"headers", "error body", "proxy CONNECT"} {
		t.Run(stage, func(t *testing.T) {
			entered := make(chan struct{})
			exited := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(exited)
				if stage == "proxy CONNECT" && r.Method != http.MethodConnect {
					t.Errorf("proxy method = %s, want CONNECT", r.Method)
				}
				if stage == "error body" {
					w.WriteHeader(http.StatusForbidden)
					w.(http.Flusher).Flush()
				}
				close(entered)
				<-r.Context().Done()
			}))
			t.Cleanup(server.Close)
			t.Cleanup(server.CloseClientConnections)
			cfg := &rest.Config{Host: server.URL}
			if stage == "proxy CONNECT" {
				proxyURL, err := url.Parse(server.URL)
				if err != nil {
					t.Fatal(err)
				}
				cfg.Host = "https://portforward.example.invalid"
				cfg.Proxy = http.ProxyURL(proxyURL)
			}
			cs, err := kubernetes.NewForConfig(cfg)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			forwarder, err := newPortForwarder(ctx, cs, cfg, "default", "pod", 0, 80, make(chan struct{}))
			if err != nil {
				t.Fatal(err)
			}
			result := make(chan error, 1)
			go func() { result <- forwarder.ForwardPorts() }()
			receivePFTest(t, entered)
			cancel()
			if err := receivePFTest(t, result); err == nil {
				t.Fatal("canceled upgrade returned success")
			}
			receivePFTest(t, exited)
		})
	}
}

func TestPortForwardUpgradePreservesTLSAuthAndSessionLifetime(t *testing.T) {
	for _, status := range []int{0, http.StatusOK, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusBadRequest, http.StatusForbidden, http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusUpgradeRequired, http.StatusNotImplemented} {
		name := "websocket"
		if status != 0 {
			name = fmt.Sprintf("spdy_after_%d", status)
		}
		t.Run(name, func(t *testing.T) {
			testPortForwardUpgrade(t, status)
		})
	}
}

func testPortForwardUpgrade(t *testing.T, websocketStatus int) {
	t.Helper()
	closed := make(chan struct{})
	requests := make(chan string, 2)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.Method
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("upgrade lost authentication")
		}
		if r.Header.Get("Impersonate-User") != "test-user" || r.Header.Get("Impersonate-Group") != "test-group" {
			t.Error("upgrade lost impersonation")
		}
		if r.Header.Get("User-Agent") != "klustr-transport-test" || r.Header.Get("X-Klustr-Test") != "wrapped" {
			t.Error("upgrade lost config wrappers")
		}
		if r.URL.Path != "/api/v1/namespaces/default/pods/pod/portforward" {
			t.Errorf("unexpected upgrade path: %s", r.URL.Path)
		}
		if r.Method == http.MethodGet && websocketStatus != 0 {
			http.Error(w, "websocket upgrade unavailable", websocketStatus)
			return
		}
		handler := func(stream httpstream.Stream, replySent <-chan struct{}) error {
			go func() {
				defer stream.Close()
				<-replySent
				if stream.Headers().Get(corev1.StreamType) == corev1.StreamTypeData {
					_, _ = io.Copy(stream, stream)
				}
			}()
			return nil
		}
		var conn httpstream.Connection
		if websocketStatus == 0 {
			if r.Method != http.MethodGet {
				t.Errorf("websocket method = %s, want GET", r.Method)
			}
			protocol := pfconstants.WebsocketsSPDYTunnelingPrefix + portforward.PortForwardProtocolV1Name
			if !slices.Contains(websocket.Subprotocols(r), protocol) {
				t.Errorf("missing websocket tunnel protocol: %v", websocket.Subprotocols(r))
			}
			upgrader := websocket.Upgrader{Subprotocols: []string{protocol}}
			ws, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				t.Error(err)
				close(closed)
				return
			}
			conn, err = httpstreamspdy.NewServerConnection(portforward.NewTunnelingConnection("test", ws), handler)
			if err != nil {
				t.Error(err)
				_ = ws.Close()
			}
		} else {
			if r.Method != http.MethodPost {
				t.Errorf("SPDY method = %s, want POST", r.Method)
			}
			w.Header().Set(httpstream.HeaderProtocolVersion, portforward.PortForwardProtocolV1Name)
			conn = httpstreamspdy.NewResponseUpgrader().UpgradeResponse(w, r, handler)
		}
		if conn == nil {
			close(closed)
			return
		}
		defer conn.Close()
		<-conn.CloseChan()
		close(closed)
	}))
	t.Cleanup(server.Close)
	var dials atomic.Int32
	cfg := &rest.Config{
		Host: server.URL, BearerToken: "test-token", UserAgent: "klustr-transport-test",
		Timeout:     time.Nanosecond,
		Impersonate: rest.ImpersonationConfig{UserName: "test-user", Groups: []string{"test-group"}},
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, address)
		},
		WrapTransport: func(rt http.RoundTripper) http.RoundTripper {
			return pfTestTransport(func(req *http.Request) (*http.Response, error) {
				req = req.Clone(req.Context())
				req.Header.Set("X-Klustr-Test", "wrapped")
				return rt.RoundTrip(req)
			})
		},
	}
	cfg.NextProtos = []string{"h2", "http/1.1"}
	cfg.CAData = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ready := make(chan struct{})
	forwarder, err := newPortForwarder(ctx, cs, cfg, "default", "pod", 0, 80, ready)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- forwarder.ForwardPorts() }()
	receivePFTest(t, ready)
	ports, err := forwarder.GetPorts()
	if err != nil || len(ports) != 1 || ports[0].Local == 0 {
		t.Fatalf("forward not listening: ports=%v err=%v", ports, err)
	}
	if isClosed(closed) {
		t.Fatal("successful upgrade closed the session")
	}
	local, err := net.DialTimeout("tcp", net.JoinHostPort("localhost", fmt.Sprint(ports[0].Local)), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	if err := local.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(local, "ping"); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(local, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("forwarded stream returned %q, err=%v", buf, err)
	}
	if method := receivePFTest(t, requests); method != http.MethodGet {
		t.Fatalf("first upgrade = %s, want GET", method)
	}
	if websocketStatus != 0 {
		if method := receivePFTest(t, requests); method != http.MethodPost {
			t.Fatalf("fallback upgrade = %s, want POST", method)
		}
	}
	if len(requests) != 0 {
		t.Fatal("unexpected additional upgrade attempt")
	}
	if dials.Load() == 0 {
		t.Fatal("upgrade ignored the configured dialer")
	}
	if cfg.Timeout != time.Nanosecond || !slices.Equal(cfg.NextProtos, []string{"h2", "http/1.1"}) {
		t.Fatal("upgrade mutated the shared REST config")
	}
	cancel()
	if err := receivePFTest(t, result); err != nil {
		t.Fatalf("stopping ready forward: %v", err)
	}
	receivePFTest(t, closed)
	if conn, err := net.DialTimeout("tcp", net.JoinHostPort("localhost", fmt.Sprint(ports[0].Local)), time.Second); err == nil {
		_ = conn.Close()
		t.Fatal("stopped port-forward retained its listener")
	}
}

func TestPortForwardReadyTimeoutCancelsWorker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		exited := make(chan struct{})
		mgr := newTestPFManager(func(ctx context.Context, _ chan struct{}) (portForwarder, error) {
			return &fakePortForwarder{run: func() error {
				defer close(exited)
				<-ctx.Done()
				return ctx.Err()
			}}, nil
		})
		defer mgr.stopAll()
		_, err := mgr.add(testPFSpec)
		if err == nil || !strings.Contains(err.Error(), "timed out") {
			t.Fatalf("start returned %v, want readiness timeout", err)
		}
		synctest.Wait()
		if !isClosed(exited) {
			t.Fatal("timed-out worker was not canceled")
		}
		if got := mgr.list(); len(got) != 1 || mgr.runningForTest(got[0].ID) {
			t.Fatal("timed-out forward is still running")
		}
	})
}
