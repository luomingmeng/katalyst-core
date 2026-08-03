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

	advisorapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuadvisor"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/util"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/metric"
)

const (
	advisorCgroupPostCommitRetryDelay   = 10 * time.Millisecond
	advisorCgroupPostCommitMaxBackoff   = time.Second
	advisorCgroupPostCommitApplyTimeout = time.Second
)

type advisorCgroupPostCommitEvent struct {
	revision uint64
	token    uint64
	response *advisorapi.ListAndWatchResponse
}

type advisorCgroupPostCommitOutbox struct {
	mu sync.Mutex
	wg sync.WaitGroup

	sequenceMu         contextMutex
	beforeSequenceLock func()
	applyTimeout       time.Duration

	send func(context.Context, advisorCgroupPostCommitEvent) error

	pending        []advisorCgroupPostCommitEvent
	latestRevision uint64
	latestToken    uint64
	wakeCh         chan struct{}

	started bool
	ctx     context.Context
	cancel  context.CancelFunc
}

func newAdvisorCgroupPostCommitOutbox(
	send func(context.Context, advisorCgroupPostCommitEvent) error,
) *advisorCgroupPostCommitOutbox {
	return &advisorCgroupPostCommitOutbox{
		send:         send,
		wakeCh:       make(chan struct{}, 1),
		sequenceMu:   newContextMutex(),
		applyTimeout: advisorCgroupPostCommitApplyTimeout,
	}
}

type contextMutex chan struct{}

func newContextMutex() contextMutex {
	m := make(contextMutex, 1)
	m <- struct{}{}
	return m
}

func (m contextMutex) lock(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m:
		return nil
	}
}

func (m contextMutex) unlock() {
	m <- struct{}{}
}

func (o *advisorCgroupPostCommitOutbox) lockSequence(ctx context.Context) error {
	if o.beforeSequenceLock != nil {
		o.beforeSequenceLock()
	}
	return o.sequenceMu.lock(ctx)
}

func (o *advisorCgroupPostCommitOutbox) start() {
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

func (o *advisorCgroupPostCommitOutbox) stop() {
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

func (o *advisorCgroupPostCommitOutbox) enqueue(event advisorCgroupPostCommitEvent) {
	o.mu.Lock()
	if event.revision < o.latestRevision {
		o.mu.Unlock()
		return
	}
	o.latestRevision = event.revision
	o.latestToken = event.token
	o.pending = append(o.pending, event)
	o.mu.Unlock()
	o.wake()
}

func (o *advisorCgroupPostCommitOutbox) advance(committedRevision uint64) {
	if err := o.lockSequence(context.Background()); err != nil {
		return
	}
	defer o.sequenceMu.unlock()
	o.advanceLocked(committedRevision)
}

func (o *advisorCgroupPostCommitOutbox) advanceLocked(committedRevision uint64) {
	o.mu.Lock()
	if committedRevision > o.latestRevision {
		o.latestRevision = committedRevision
		o.latestToken = 0
	}
	o.mu.Unlock()
	o.wake()
}

func (o *advisorCgroupPostCommitOutbox) linearizeTargetCommit(
	ctx context.Context,
	commit func() (uint64, error),
) error {
	if err := o.lockSequence(ctx); err != nil {
		return err
	}
	defer o.sequenceMu.unlock()

	committedRevision, err := commit()
	if err != nil {
		return err
	}
	o.advanceLocked(committedRevision)
	return nil
}

func (o *advisorCgroupPostCommitOutbox) wake() {
	select {
	case o.wakeCh <- struct{}{}:
	default:
	}
}

func (o *advisorCgroupPostCommitOutbox) nextEvent() (advisorCgroupPostCommitEvent, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.pending) == 0 {
		return advisorCgroupPostCommitEvent{}, false
	}
	return o.pending[0], true
}

func (o *advisorCgroupPostCommitOutbox) isFresh(event advisorCgroupPostCommitEvent) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return event.revision == o.latestRevision && event.token == o.latestToken
}

func (o *advisorCgroupPostCommitOutbox) acknowledge(event advisorCgroupPostCommitEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.pending) == 0 || o.pending[0].revision != event.revision {
		return
	}
	o.pending = o.pending[1:]
}

func (o *advisorCgroupPostCommitOutbox) run(ctx context.Context) {
	defer o.wg.Done()

	var (
		failedRevision uint64
		backoff        time.Duration
	)
	for {
		event, ok := o.nextEvent()
		if !ok {
			failedRevision = 0
			backoff = 0
			select {
			case <-ctx.Done():
				return
			case <-o.wakeCh:
				continue
			}
		}
		attempted, err := o.sendIfFresh(ctx, event)
		if !attempted {
			o.acknowledge(event)
			continue
		}
		if event.revision != failedRevision {
			failedRevision = event.revision
			backoff = advisorCgroupPostCommitRetryDelay
		}
		if err == nil {
			o.acknowledge(event)
			failedRevision = 0
			backoff = 0
			continue
		}
		if !o.isFresh(event) {
			o.acknowledge(event)
			failedRevision = 0
			backoff = 0
			continue
		}
		if !o.waitForRetry(ctx, event, backoff) {
			return
		}
		if backoff < advisorCgroupPostCommitMaxBackoff {
			backoff *= 2
			if backoff > advisorCgroupPostCommitMaxBackoff {
				backoff = advisorCgroupPostCommitMaxBackoff
			}
		}
	}
}

func (o *advisorCgroupPostCommitOutbox) sendIfFresh(
	ctx context.Context,
	event advisorCgroupPostCommitEvent,
) (bool, error) {
	applyCtx, cancel := context.WithTimeout(ctx, o.applyTimeout)
	defer cancel()
	if err := o.lockSequence(applyCtx); err != nil {
		if ctx.Err() != nil {
			return false, nil
		}
		return true, err
	}
	defer o.sequenceMu.unlock()
	if ctx.Err() != nil {
		return false, nil
	}
	if !o.isFresh(event) {
		return false, nil
	}
	return true, o.send(applyCtx, event)
}

func (o *advisorCgroupPostCommitOutbox) waitForRetry(
	ctx context.Context,
	failed advisorCgroupPostCommitEvent,
	backoff time.Duration,
) bool {
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return true
		case <-o.wakeCh:
			if !o.isFresh(failed) {
				return true
			}
		}
	}
}

func (p *DynamicPolicy) advisorCgroupPostCommitOutboxInstance() *advisorCgroupPostCommitOutbox {
	p.advisorCgroupPostCommitMu.Lock()
	defer p.advisorCgroupPostCommitMu.Unlock()
	if p.advisorCgroupPostCommitOutbox == nil {
		p.advisorCgroupPostCommitOutbox = newAdvisorCgroupPostCommitOutbox(p.sendAdvisorCgroupPostCommitEvent)
	}
	return p.advisorCgroupPostCommitOutbox
}

func (p *DynamicPolicy) enqueueAdvisorCgroupPostCommit(
	revision, token uint64,
	response *advisorapi.ListAndWatchResponse,
) {
	if response == nil {
		return
	}
	cloned, ok := proto.Clone(response).(*advisorapi.ListAndWatchResponse)
	if !ok || cloned == nil {
		return
	}
	p.advisorCgroupPostCommitOutboxInstance().enqueue(advisorCgroupPostCommitEvent{
		revision: revision,
		token:    token,
		response: cloned,
	})
}

func (p *DynamicPolicy) advanceAdvisorCgroupPostCommitFence(committedRevision uint64) {
	p.advisorCgroupPostCommitMu.Lock()
	outbox := p.advisorCgroupPostCommitOutbox
	p.advisorCgroupPostCommitMu.Unlock()
	if outbox != nil {
		outbox.advance(committedRevision)
	}
}

func (p *DynamicPolicy) sendAdvisorCgroupPostCommitEvent(
	ctx context.Context,
	event advisorCgroupPostCommitEvent,
) error {
	err := p.applyCgroupConfigs(ctx, event.response)
	status := "succeeded"
	if err != nil {
		status = "failed"
	}
	if p.emitter != nil {
		tags := []metrics.MetricTag{
			{Key: "status", Val: status},
			{Key: "revision", Val: metric.MetricTagValueFormat(event.revision)},
			{Key: "token", Val: metric.MetricTagValueFormat(event.token)},
		}
		if err != nil {
			tags = append(tags, metrics.MetricTag{Key: "error_message", Val: metric.MetricTagValueFormat(err)})
		}
		_ = p.emitter.StoreInt64(
			util.MetricNameAdvisorCgroupPostCommitApply, 1, metrics.MetricTypeNameCount, tags...)
	}
	if err != nil {
		general.ErrorS(err, "post-commit advisor cgroup apply failed; durable target remains committed",
			"revision", event.revision, "token", event.token)
		return err
	}
	general.InfoS("post-commit advisor cgroup apply succeeded",
		"revision", event.revision, "token", event.token)
	return nil
}
