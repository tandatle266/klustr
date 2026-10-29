package kube

import (
	"context"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

// connectPortForward resolves a forward's pod and opens a connection to it.
// A Service is resolved on every attempt, so a reconnect lands on whichever
// pod backs it now.
func (m *ClientManager) connectPortForward(ctx context.Context, spec PortForwardSpec, localPort uint16, ready chan struct{}) (pfAttempt, error) {
	cs, err := m.Clientset(spec.Context)
	if err != nil {
		return pfAttempt{}, err
	}
	cfg, err := m.restConfig(spec.Context)
	if err != nil {
		return pfAttempt{}, err
	}
	pod, port, err := resolvePFTarget(ctx, cs, spec)
	if err != nil {
		return pfAttempt{}, err
	}
	fw, err := newPortForwarder(ctx, cs, cfg, spec.Namespace, pod.Name, localPort, port, ready)
	if err != nil {
		return pfAttempt{}, err
	}
	return pfAttempt{
		forwarder: fw,
		pod:       pod.Name,
		port:      port,
		lost:      watchPFPod(ctx, cs, spec.Namespace, pod.Name, pod.ResourceVersion),
	}, nil
}

func resolvePFTarget(ctx context.Context, cs kubernetes.Interface, spec PortForwardSpec) (*corev1.Pod, uint16, error) {
	switch spec.Kind {
	case "Pod":
		pod, err := cs.CoreV1().Pods(spec.Namespace).Get(ctx, spec.Name, metav1.GetOptions{})
		if err != nil {
			return nil, 0, err
		}
		if !pfPodUsable(pod) {
			return nil, 0, fmt.Errorf("pod %s is not running", pod.Name)
		}
		return pod, spec.Port, nil
	case "Service":
		svc, err := cs.CoreV1().Services(spec.Namespace).Get(ctx, spec.Name, metav1.GetOptions{})
		if err != nil {
			return nil, 0, err
		}
		var svcPort *corev1.ServicePort
		for i := range svc.Spec.Ports {
			if svc.Spec.Ports[i].Port == int32(spec.Port) {
				svcPort = &svc.Spec.Ports[i]
				break
			}
		}
		if svcPort == nil {
			return nil, 0, fmt.Errorf("service %s has no port %d", svc.Name, spec.Port)
		}
		if svcPort.Protocol != "" && svcPort.Protocol != corev1.ProtocolTCP {
			return nil, 0, fmt.Errorf("port-forward supports TCP only, port %d is %s", spec.Port, svcPort.Protocol)
		}
		if len(svc.Spec.Selector) == 0 {
			return nil, 0, fmt.Errorf("service %s has no selector to find a pod", svc.Name)
		}
		pods, err := cs.CoreV1().Pods(spec.Namespace).List(ctx, metav1.ListOptions{
			LabelSelector: labels.SelectorFromSet(svc.Spec.Selector).String(),
		})
		if err != nil {
			return nil, 0, err
		}
		pod := pickServicePod(pods.Items)
		if pod == nil {
			return nil, 0, fmt.Errorf("no running pod backs service %s", svc.Name)
		}
		port, err := serviceTargetPort(*svcPort, pod)
		if err != nil {
			return nil, 0, err
		}
		return pod, port, nil
	}
	return nil, 0, fmt.Errorf("cannot port-forward a %s", spec.Kind)
}

func pfPodUsable(pod *corev1.Pod) bool {
	return pod.DeletionTimestamp == nil && pod.Status.Phase == corev1.PodRunning
}

func podIsReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// pickServicePod picks a running pod, preferring ready ones, then the oldest
// so repeated resolves agree while the set is stable.
func pickServicePod(pods []corev1.Pod) *corev1.Pod {
	candidates := make([]*corev1.Pod, 0, len(pods))
	for i := range pods {
		if pfPodUsable(&pods[i]) {
			candidates = append(candidates, &pods[i])
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if ra, rb := podIsReady(a), podIsReady(b); ra != rb {
			return ra
		}
		if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
			return a.CreationTimestamp.Before(&b.CreationTimestamp)
		}
		return a.Name < b.Name
	})
	return candidates[0]
}

// serviceTargetPort maps a Service port to the container port on pod, the
// way kubectl port-forward svc/... does.
func serviceTargetPort(svcPort corev1.ServicePort, pod *corev1.Pod) (uint16, error) {
	tp := svcPort.TargetPort
	if tp.Type == intstr.Int {
		if tp.IntVal == 0 {
			return uint16(svcPort.Port), nil
		}
		return uint16(tp.IntVal), nil
	}
	protocol := svcPort.Protocol
	if protocol == "" {
		protocol = corev1.ProtocolTCP
	}
	for _, c := range pod.Spec.Containers {
		for _, p := range c.Ports {
			pp := p.Protocol
			if pp == "" {
				pp = corev1.ProtocolTCP
			}
			if p.Name == tp.StrVal && pp == protocol {
				return uint16(p.ContainerPort), nil
			}
		}
	}
	return 0, fmt.Errorf("pod %s has no port named %q", pod.Name, tp.StrVal)
}

// watchPFPod closes the returned channel once the pod is deleted or stops
// running. A watch that cannot start (RBAC) leaves it open; the forward then
// notices only when the stream itself fails.
func watchPFPod(ctx context.Context, cs kubernetes.Interface, namespace, name, resourceVersion string) <-chan struct{} {
	lost := make(chan struct{})
	go func() {
		rv := resourceVersion
		for {
			w, err := cs.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{
				FieldSelector:   fields.OneTermEqualSelector("metadata.name", name).String(),
				ResourceVersion: rv,
			})
			if err != nil {
				return
			}
			gone := false
			for ev := range w.ResultChan() {
				switch ev.Type {
				case watch.Deleted:
					gone = true
				case watch.Added, watch.Modified:
					if pod, ok := ev.Object.(*corev1.Pod); ok {
						rv = pod.ResourceVersion
						gone = !pfPodUsable(pod)
					}
				case watch.Error:
					// Usually an expired resourceVersion; rewatch from now.
					rv = ""
				}
				if gone {
					break
				}
			}
			w.Stop()
			if gone {
				close(lost)
				return
			}
			// The server ends watches on a timeout; pick it up again.
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}()
	return lost
}
