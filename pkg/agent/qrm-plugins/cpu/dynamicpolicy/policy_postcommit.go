/*
Copyright 2026 The Katalyst Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dynamicpolicy

import (
	"context"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/advisorsvc"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/util"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/metric"
)

const (
	advisorPostCommitRPCTimeout = time.Second
	advisorPostCommitRetryDelay = 10 * time.Millisecond
	advisorPostCommitMaxBackoff = time.Second
)

type advisorPostCommitOperation string

const (
	advisorPostCommitAdd    advisorPostCommitOperation = "add"
	advisorPostCommitRemove advisorPostCommitOperation = "remove"
)

type advisorPostCommitEvent struct {
	revision  uint64
	operation advisorPostCommitOperation
	podUID    string
	add       *advisorsvc.ContainerMetadata
}

type advisorPostCommitRetry struct {
	revision uint64
	backoff  time.Duration
	readyAt  time.Time
}

type advisorPostCommitOutbox struct {
	mu sync.Mutex
	wg sync.WaitGroup

	send       func(context.Context, advisorPostCommitEvent) error
	deadLetter func(advisorPostCommitEvent, error)
	now        func() time.Time

	// afterRetryScan is a synchronization hook for tests that need to move time
	// between observing a future retry and calculating its remaining delay.
	afterRetryScan func()

	pending      map[string][]advisorPostCommitEvent
	podOrder     []string
	queued       map[string]bool
	retries      map[string]advisorPostCommitRetry
	nextRevision uint64
	wakeCh       chan struct{}

	started bool
	ctx     context.Context
	cancel  context.CancelFunc
}

func newAdvisorPostCommitOutbox(
	send func(context.Context, advisorPostCommitEvent) error,
) *advisorPostCommitOutbox {
	return &advisorPostCommitOutbox{
		send:    send,
		now:     time.Now,
		pending: make(map[string][]advisorPostCommitEvent),
		queued:  make(map[string]bool),
		retries: make(map[string]advisorPostCommitRetry),
		wakeCh:  make(chan struct{}, 1),
	}
}

func (o *advisorPostCommitOutbox) start() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.started {
		return
	}
	o.ctx, o.cancel = context.WithCancel(context.Background())
	o.started = true
	o.wg.Add(1)
	go o.run(o.ctx)
}

func (o *advisorPostCommitOutbox) stop() {
	o.mu.Lock()
	if !o.started {
		o.mu.Unlock()
		return
	}
	cancel := o.cancel
	o.started = false
	o.cancel = nil
	o.ctx = nil
	o.mu.Unlock()

	cancel()
	o.wg.Wait()
}

func (o *advisorPostCommitOutbox) enqueueAdd(request *advisorsvc.ContainerMetadata) {
	o.mu.Lock()
	o.nextRevision++
	event := advisorPostCommitEvent{
		revision:  o.nextRevision,
		operation: advisorPostCommitAdd,
		podUID:    request.PodUid,
		add:       request,
	}
	o.pending[event.podUID] = append(o.pending[event.podUID], event)
	o.queuePodLocked(event.podUID)
	o.mu.Unlock()
	o.wake()
}

func (o *advisorPostCommitOutbox) enqueueRemove(podUID string) {
	o.mu.Lock()
	o.nextRevision++
	event := advisorPostCommitEvent{
		revision:  o.nextRevision,
		operation: advisorPostCommitRemove,
		podUID:    podUID,
	}
	events := o.pending[podUID]
	merged := make([]advisorPostCommitEvent, 0, len(events)+1)
	merged = append(merged, event)
	for _, pending := range events {
		if pending.revision > event.revision {
			merged = append(merged, pending)
		}
	}
	o.pending[podUID] = merged
	o.queuePodLocked(podUID)
	o.mu.Unlock()
	o.wake()
}

func (o *advisorPostCommitOutbox) queuePodLocked(podUID string) {
	if o.queued[podUID] {
		return
	}
	o.queued[podUID] = true
	o.podOrder = append(o.podOrder, podUID)
}

func (o *advisorPostCommitOutbox) wake() {
	select {
	case o.wakeCh <- struct{}{}:
	default:
	}
}

func (o *advisorPostCommitOutbox) nextEvent() (advisorPostCommitEvent, bool, bool, time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	now := o.now()
	var next advisorPostCommitEvent
	var earliestRetry time.Time
	found := false
	for _, podUID := range o.podOrder {
		events := o.pending[podUID]
		if len(events) == 0 {
			continue
		}
		retry, retrying := o.retries[podUID]
		if retrying && retry.revision != events[0].revision {
			delete(o.retries, podUID)
			retrying = false
		}
		if retrying && now.Before(retry.readyAt) {
			if earliestRetry.IsZero() || retry.readyAt.Before(earliestRetry) {
				earliestRetry = retry.readyAt
			}
			continue
		}
		if !found || events[0].revision < next.revision {
			next = events[0]
			found = true
		}
	}
	if found {
		return next, true, false, 0
	}
	if earliestRetry.IsZero() {
		return advisorPostCommitEvent{}, false, false, 0
	}
	if o.afterRetryScan != nil {
		o.afterRetryScan()
	}
	return advisorPostCommitEvent{}, false, true, earliestRetry.Sub(o.now())
}

func (o *advisorPostCommitOutbox) acknowledge(event advisorPostCommitEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	events := o.pending[event.podUID]
	if len(events) == 0 || events[0].revision != event.revision {
		return
	}
	events = events[1:]
	delete(o.retries, event.podUID)
	if len(events) > 0 {
		o.pending[event.podUID] = events
		return
	}
	delete(o.pending, event.podUID)
	delete(o.queued, event.podUID)
	for i, podUID := range o.podOrder {
		if podUID == event.podUID {
			o.podOrder = append(o.podOrder[:i], o.podOrder[i+1:]...)
			break
		}
	}
}

func (o *advisorPostCommitOutbox) run(ctx context.Context) {
	defer o.wg.Done()

	for {
		event, ok, hasRetry, retryAfter := o.nextEvent()
		if !ok {
			if hasRetry && retryAfter <= 0 {
				continue
			}
			if !o.waitForWork(ctx, hasRetry, retryAfter) {
				return
			}
			continue
		}

		err := o.send(ctx, event)
		if err == nil {
			o.acknowledge(event)
			continue
		}
		if isPermanentAdvisorPostCommitError(err) {
			if o.deadLetter != nil {
				o.deadLetter(event, err)
			}
			o.acknowledge(event)
			continue
		}
		o.scheduleRetry(event)
	}
}

func (o *advisorPostCommitOutbox) waitForWork(
	ctx context.Context,
	hasRetry bool,
	retryAfter time.Duration,
) bool {
	if !hasRetry {
		select {
		case <-ctx.Done():
			return false
		case <-o.wakeCh:
			return true
		}
	}
	timer := time.NewTimer(retryAfter)
	defer stopAndDrainTimer(timer)
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	case <-o.wakeCh:
		return true
	}
}

func (o *advisorPostCommitOutbox) scheduleRetry(failed advisorPostCommitEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	events := o.pending[failed.podUID]
	if len(events) == 0 || events[0].revision != failed.revision {
		return
	}
	backoff := advisorPostCommitRetryDelay
	if previous, ok := o.retries[failed.podUID]; ok && previous.revision == failed.revision {
		backoff = previous.backoff * 2
		if backoff > advisorPostCommitMaxBackoff {
			backoff = advisorPostCommitMaxBackoff
		}
	}
	o.retries[failed.podUID] = advisorPostCommitRetry{
		revision: failed.revision,
		backoff:  backoff,
		readyAt:  o.now().Add(backoff),
	}
}

func stopAndDrainTimer(timer *time.Timer) {
	if timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}

func isPermanentAdvisorPostCommitError(err error) bool {
	switch status.Code(err) {
	case codes.InvalidArgument,
		codes.NotFound,
		codes.AlreadyExists,
		codes.PermissionDenied,
		codes.FailedPrecondition,
		codes.OutOfRange,
		codes.Unimplemented,
		codes.Unauthenticated:
		return true
	default:
		return false
	}
}

func (p *DynamicPolicy) advisorPostCommitOutboxInstance() *advisorPostCommitOutbox {
	p.advisorPostCommitMu.Lock()
	defer p.advisorPostCommitMu.Unlock()
	if p.advisorPostCommitOutbox == nil {
		p.advisorPostCommitOutbox = newAdvisorPostCommitOutbox(p.sendAdvisorPostCommitEvent)
		p.advisorPostCommitOutbox.deadLetter = p.observeAdvisorPostCommitDeadLetter
	}
	return p.advisorPostCommitOutbox
}

func (p *DynamicPolicy) startAdvisorPostCommitWorker() {
	p.advisorPostCommitOutboxInstance().start()
	p.advisorCgroupPostCommitOutboxInstance().start()
}

func (p *DynamicPolicy) stopAdvisorPostCommitWorker() {
	p.advisorPostCommitMu.Lock()
	outbox := p.advisorPostCommitOutbox
	p.advisorPostCommitMu.Unlock()
	if outbox != nil {
		outbox.stop()
	}
	p.advisorCgroupPostCommitMu.Lock()
	cgroupOutbox := p.advisorCgroupPostCommitOutbox
	p.advisorCgroupPostCommitMu.Unlock()
	if cgroupOutbox != nil {
		cgroupOutbox.stop()
	}
}

func (p *DynamicPolicy) enqueueAdvisorAdd(metadataRequest *advisorsvc.ContainerMetadata) {
	if metadataRequest == nil {
		return
	}
	request, ok := proto.Clone(metadataRequest).(*advisorsvc.ContainerMetadata)
	if !ok || request == nil {
		return
	}
	p.advisorPostCommitOutboxInstance().enqueueAdd(request)
}

func (p *DynamicPolicy) enqueueAdvisorRemove(podUID string) {
	if podUID == "" {
		return
	}
	p.advisorPostCommitOutboxInstance().enqueueRemove(podUID)
}

func (p *DynamicPolicy) sendAdvisorPostCommitEvent(
	parent context.Context,
	event advisorPostCommitEvent,
) error {
	ctx, cancel := context.WithTimeout(parent, advisorPostCommitRPCTimeout)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(
		ctx,
		util.AdvisorRPCMetadataKeySupportsGetAdvice,
		util.AdvisorRPCMetadataValueSupportsGetAdvice,
	)

	var err error
	switch event.operation {
	case advisorPostCommitAdd:
		_, err = p.advisorClient.AddContainer(ctx, event.add)
	case advisorPostCommitRemove:
		_, err = p.advisorClient.RemovePod(ctx, &advisorsvc.RemovePodRequest{PodUid: event.podUID})
	}

	status := "succeeded"
	if err != nil {
		status = "failed"
	}
	if p.emitter != nil {
		tags := []metrics.MetricTag{
			{Key: "operation", Val: string(event.operation)},
			{Key: "status", Val: status},
			{Key: "revision", Val: metric.MetricTagValueFormat(event.revision)},
		}
		if err != nil {
			tags = append(tags, metrics.MetricTag{Key: "error_message", Val: metric.MetricTagValueFormat(err)})
		}
		_ = p.emitter.StoreInt64(
			util.MetricNameAdvisorPostCommitNotification, 1, metrics.MetricTypeNameCount, tags...)
	}
	if err != nil {
		general.ErrorS(err, "post-commit advisor notification failed; durable state remains committed",
			"operation", event.operation, "revision", event.revision,
			"podUID", event.podUID, "containerName", event.containerName())
		return err
	}
	general.InfoS("post-commit advisor notification succeeded",
		"operation", event.operation, "revision", event.revision,
		"podUID", event.podUID, "containerName", event.containerName())
	return nil
}

func (p *DynamicPolicy) observeAdvisorPostCommitDeadLetter(event advisorPostCommitEvent, err error) {
	code := status.Code(err)
	if p.emitter != nil {
		_ = p.emitter.StoreInt64(
			util.MetricNameAdvisorPostCommitNotification,
			1,
			metrics.MetricTypeNameCount,
			metrics.MetricTag{Key: "operation", Val: string(event.operation)},
			metrics.MetricTag{Key: "status", Val: "dead_lettered"},
			metrics.MetricTag{Key: "revision", Val: metric.MetricTagValueFormat(event.revision)},
			metrics.MetricTag{Key: "grpc_code", Val: code.String()},
		)
	}
	general.ErrorS(err, "post-commit advisor notification moved to dead letter",
		"operation", event.operation, "revision", event.revision,
		"podUID", event.podUID, "containerName", event.containerName(),
		"grpcCode", code.String())
}

func (e advisorPostCommitEvent) containerName() string {
	if e.add == nil {
		return ""
	}
	return e.add.ContainerName
}
