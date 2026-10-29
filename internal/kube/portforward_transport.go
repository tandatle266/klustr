package kube

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	pfconstants "k8s.io/apimachinery/pkg/util/portforward"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	clientspdy "k8s.io/client-go/transport/spdy"
	clientwebsocket "k8s.io/client-go/transport/websocket"
	"k8s.io/streaming/pkg/httpstream"
	httpstreamspdy "k8s.io/streaming/pkg/httpstream/spdy"
)

type portForwarder interface {
	ForwardPorts() error
	GetPorts() ([]portforward.ForwardedPort, error)
}

type pfDialer func(...string) (httpstream.Connection, string, error)

func (d pfDialer) Dial(protocols ...string) (httpstream.Connection, string, error) {
	return d(protocols...)
}

func newPortForwarder(
	ctx context.Context,
	cs *kubernetes.Clientset,
	restCfg *rest.Config,
	namespace, podName string,
	localPort, remotePort uint16,
	readyCh chan struct{},
) (portForwarder, error) {
	transport, upgrader, err := newStreamingSPDYTransport(ctx, restCfg)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Transport: transport}
	url := cs.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(namespace).
		Name(podName).
		SubResource("portforward").
		URL()
	spdyDialer := pfDialer(func(protocols ...string) (httpstream.Connection, string, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url.String(), nil)
		if err != nil {
			return nil, "", err
		}
		return clientspdy.NegotiateStreaming(upgrader, client, req, protocols...)
	})
	websocketDialer, err := newPFWebSocketDialer(ctx, restCfg, url)
	if err != nil {
		return nil, err
	}
	dialer := portforward.NewFallbackDialerForStreaming(websocketDialer, spdyDialer, func(err error) bool {
		return ctx.Err() == nil && shouldFallbackStreaming(err)
	})
	ports := []string{fmt.Sprintf("%d:%d", localPort, remotePort)}
	return portforward.NewOnAddressesForStreamingWithContext(ctx, dialer, []string{"localhost"}, ports, readyCh, io.Discard, io.Discard)
}

func newPFWebSocketDialer(ctx context.Context, cfg *rest.Config, endpoint *url.URL) (httpstream.Dialer, error) {
	transport, holder, err := clientwebsocket.RoundTripperFor(streamingWebSocketConfig(ctx, cfg))
	if err != nil {
		return nil, err
	}
	return pfDialer(func(protocols ...string) (httpstream.Connection, string, error) {
		// The upstream tunneling dialer creates a background-context request.
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return nil, "", err
		}
		tunneled := make([]string, len(protocols))
		for i, protocol := range protocols {
			tunneled[i] = pfconstants.WebsocketsSPDYTunnelingPrefix + protocol
		}
		conn, err := clientwebsocket.Negotiate(transport, holder, req, tunneled...)
		if err != nil {
			return nil, "", err
		}
		stream, err := httpstreamspdy.NewClientConnectionWithPings(portforward.NewTunnelingConnection("client", conn), portforward.PingPeriod)
		if err != nil {
			_ = conn.Close()
			return nil, "", err
		}
		return stream, strings.TrimPrefix(conn.Subprotocol(), pfconstants.WebsocketsSPDYTunnelingPrefix), nil
	}), nil
}
