// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kube // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/k8sattributesprocessor/internal/kube"

import (
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"
	api_v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/kubelet"
)

// KubeletPodSourceConfig configures a kubelet-based pod source. When
// supplied, the WatchClient swaps its API-server pod informer for a polling
// loop against the local kubelet /pods endpoint, while keeping the other
// (namespace/node/workload) informers on the API server.
type KubeletPodSourceConfig struct {
	// Endpoint is the kubelet endpoint (e.g. "https://${HOST_IP}:10250"). If
	// empty, the node hostname is used (see internal/kubelet).
	Endpoint string
	// Client is the kubelet client config (auth, TLS).
	Client kubelet.ClientConfig
	// PollInterval is how often to poll the kubelet /pods endpoint.
	PollInterval time.Duration
}

// kubeletPodSource polls the local kubelet /pods endpoint and feeds
// pod add/update/delete events into the WatchClient cache.
type kubeletPodSource struct {
	client       kubelet.Client
	pollInterval time.Duration
	logger       *zap.Logger

	// seen retains the last-observed copy of each pod, keyed by UID, so that
	// we can synthesize a delete event for pods that disappear between polls.
	// The retained pod is needed because WatchClient.forgetPod relies on
	// fields beyond the UID to locate the cache entry.
	seen map[types.UID]*api_v1.Pod
}

// newKubeletPodSource builds a kubelet client and returns a pod source that
// can be driven via run.
func newKubeletPodSource(cfg KubeletPodSourceConfig, logger *zap.Logger) (*kubeletPodSource, error) {
	provider, err := kubelet.NewClientProvider(cfg.Endpoint, &cfg.Client, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubelet client provider: %w", err)
	}
	c, err := provider.BuildClient()
	if err != nil {
		return nil, fmt.Errorf("failed to build kubelet client: %w", err)
	}
	return &kubeletPodSource{
		client:       c,
		pollInterval: cfg.PollInterval,
		logger:       logger,
		seen:         map[types.UID]*api_v1.Pod{},
	}, nil
}

// run drives the polling loop until stopCh is closed. onAdd is called for
// each pod returned by the kubelet on every poll, and onDelete is called
// for pods that have disappeared since the previous poll.
func (s *kubeletPodSource) run(stopCh <-chan struct{}, onAdd, onDelete func(*api_v1.Pod)) {
	s.poll(onAdd, onDelete)

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			s.poll(onAdd, onDelete)
		}
	}
}

func (s *kubeletPodSource) poll(onAdd, onDelete func(*api_v1.Pod)) {
	body, err := s.client.Get("/pods")
	if err != nil {
		s.logger.Warn("failed to fetch pods from kubelet", zap.Error(err))
		return
	}

	var list api_v1.PodList
	if err := json.Unmarshal(body, &list); err != nil {
		s.logger.Warn("failed to unmarshal kubelet pods response", zap.Error(err))
		return
	}

	current := make(map[types.UID]*api_v1.Pod, len(list.Items))
	for i := range list.Items {
		pod := &list.Items[i]
		current[pod.UID] = pod
		onAdd(pod)
	}

	for uid, prev := range s.seen {
		if _, ok := current[uid]; ok {
			continue
		}
		onDelete(prev)
	}
	s.seen = current
}
