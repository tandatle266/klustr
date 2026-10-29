package kube

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
)

func pfTestPod(name string, phase corev1.PodPhase, ready bool, age time.Duration) corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "default",
			Labels:            map[string]string{"app": "web"},
			CreationTimestamp: metav1.NewTime(time.Unix(1_700_000_000, 0).Add(-age)),
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name:  "app",
			Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}, {Name: "dns", ContainerPort: 53, Protocol: corev1.ProtocolUDP}},
		}}},
		Status: corev1.PodStatus{Phase: phase, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: status}}},
	}
}

func TestPickServicePodPrefersReadyThenOldest(t *testing.T) {
	deleting := pfTestPod("deleting", corev1.PodRunning, true, 9*time.Hour)
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	pods := []corev1.Pod{
		pfTestPod("pending", corev1.PodPending, false, 10*time.Hour),
		deleting,
		pfTestPod("unready-old", corev1.PodRunning, false, 8*time.Hour),
		pfTestPod("ready-new", corev1.PodRunning, true, time.Hour),
		pfTestPod("ready-old", corev1.PodRunning, true, 2*time.Hour),
	}
	if got := pickServicePod(pods); got == nil || got.Name != "ready-old" {
		t.Fatalf("picked %v, want ready-old", got)
	}
	if got := pickServicePod(pods[:3]); got == nil || got.Name != "unready-old" {
		t.Fatalf("picked %v, want the running pod even when unready", got)
	}
	if got := pickServicePod(pods[:2]); got != nil {
		t.Fatalf("picked %s from pods that cannot serve", got.Name)
	}
}

func TestServiceTargetPort(t *testing.T) {
	pod := pfTestPod("web-0", corev1.PodRunning, true, 0)
	cases := []struct {
		name    string
		port    corev1.ServicePort
		want    uint16
		wantErr string
	}{
		{"number", corev1.ServicePort{Port: 80, TargetPort: intstr.FromInt32(8080)}, 8080, ""},
		{"unset target uses the port", corev1.ServicePort{Port: 80}, 80, ""},
		{"named", corev1.ServicePort{Port: 80, TargetPort: intstr.FromString("http")}, 8080, ""},
		{"named with another protocol", corev1.ServicePort{Port: 53, TargetPort: intstr.FromString("dns")}, 0, `no port named "dns"`},
		{"missing name", corev1.ServicePort{Port: 80, TargetPort: intstr.FromString("grpc")}, 0, `no port named "grpc"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := serviceTargetPort(tc.port, &pod)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("got %d, %v; want %d", got, err, tc.want)
			}
		})
	}
}

func TestResolvePFTarget(t *testing.T) {
	ready := pfTestPod("web-ready", corev1.PodRunning, true, time.Hour)
	done := pfTestPod("web-done", corev1.PodSucceeded, false, 2*time.Hour)
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default"},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "web"},
			Ports: []corev1.ServicePort{
				{Port: 80, TargetPort: intstr.FromString("http"), Protocol: corev1.ProtocolTCP},
				{Port: 53, Protocol: corev1.ProtocolUDP},
			},
		},
	}
	headless := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "external", Namespace: "default"},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 443}}},
	}
	cs := fake.NewClientset(&ready, &done, svc, headless)
	ctx := context.Background()
	spec := func(kind, name string, port uint16) PortForwardSpec {
		return PortForwardSpec{Context: "ctx", Namespace: "default", Kind: kind, Name: name, Port: port}
	}

	pod, port, err := resolvePFTarget(ctx, cs, spec("Service", "web", 80))
	if err != nil || pod.Name != "web-ready" || port != 8080 {
		t.Fatalf("service resolved to %v:%d, %v", pod, port, err)
	}
	pod, port, err = resolvePFTarget(ctx, cs, spec("Pod", "web-ready", 9000))
	if err != nil || pod.Name != "web-ready" || port != 9000 {
		t.Fatalf("pod resolved to %v:%d, %v", pod, port, err)
	}
	for _, tc := range []struct {
		spec PortForwardSpec
		want string
	}{
		{spec("Pod", "web-done", 80), "not running"},
		{spec("Pod", "missing", 80), "not found"},
		{spec("Service", "web", 8443), "has no port 8443"},
		{spec("Service", "web", 53), "TCP only"},
		{spec("Service", "external", 443), "no selector"},
		{spec("Deployment", "web", 80), "cannot port-forward"},
	} {
		if _, _, err := resolvePFTarget(ctx, cs, tc.spec); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s %s:%d err = %v, want %q", tc.spec.Kind, tc.spec.Name, tc.spec.Port, err, tc.want)
		}
	}

	if err := cs.CoreV1().Pods("default").Delete(ctx, "web-ready", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := resolvePFTarget(ctx, cs, spec("Service", "web", 80)); err == nil || !strings.Contains(err.Error(), "no running pod") {
		t.Fatalf("service without a running pod: %v", err)
	}
}

func TestWatchPFPodClosesWhenThePodGoes(t *testing.T) {
	pod := pfTestPod("web-0", corev1.PodRunning, true, 0)
	cs := fake.NewClientset(&pod)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lost := watchPFPod(ctx, cs, "default", "web-0", "")
	// The fake watch only delivers events sent after it is registered.
	deadline := time.Now().Add(3 * time.Second)
	for {
		if err := cs.CoreV1().Pods("default").Delete(ctx, "web-0", metav1.DeleteOptions{}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-lost:
			return
		case <-time.After(50 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("deleting the pod did not close lost")
		}
		if _, err := cs.CoreV1().Pods("default").Create(ctx, &pod, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
}
