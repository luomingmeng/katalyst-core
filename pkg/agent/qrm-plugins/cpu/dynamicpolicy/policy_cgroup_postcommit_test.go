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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAdvisorCgroupOutboxRejectsFailedStaleResponseBeforeRetry(t *testing.T) {
	firstAttempt := make(chan struct{})
	releaseFirst := make(chan struct{})
	var (
		mu       sync.Mutex
		attempts []uint64
		applied  []uint64
	)

	outbox := newAdvisorCgroupPostCommitOutbox(func(_ context.Context, event advisorCgroupPostCommitEvent) error {
		mu.Lock()
		attempts = append(attempts, event.revision)
		attempt := len(attempts)
		mu.Unlock()
		if attempt == 1 {
			close(firstAttempt)
			<-releaseFirst
			return errors.New("controlled first apply failure")
		}
		mu.Lock()
		applied = append(applied, event.revision)
		mu.Unlock()
		return nil
	})
	outbox.start()
	t.Cleanup(outbox.stop)

	outbox.enqueue(advisorCgroupPostCommitEvent{revision: 1, token: 11})
	<-firstAttempt
	outbox.enqueue(advisorCgroupPostCommitEvent{revision: 2, token: 12})
	close(releaseFirst)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(applied) == 1
	}, time.Second, time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []uint64{1, 2}, attempts,
		"the failed old response must be rejected rather than retried after a newer commit")
	require.Equal(t, []uint64{2}, applied)
}

func TestAdvisorCgroupOutboxRetriesLatestResponse(t *testing.T) {
	var (
		mu       sync.Mutex
		attempts int
	)
	outbox := newAdvisorCgroupPostCommitOutbox(func(_ context.Context, event advisorCgroupPostCommitEvent) error {
		mu.Lock()
		defer mu.Unlock()
		attempts++
		if attempts == 1 {
			return errors.New("temporary apply failure")
		}
		require.Equal(t, uint64(7), event.revision)
		require.Equal(t, uint64(19), event.token)
		return nil
	})
	outbox.start()
	t.Cleanup(outbox.stop)

	outbox.enqueue(advisorCgroupPostCommitEvent{revision: 7, token: 19})

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return attempts == 2
	}, time.Second, time.Millisecond)
}

func TestAdvisorCgroupOutboxRejectsResponseStaleAfterUnrelatedCommit(t *testing.T) {
	firstAttempt := make(chan struct{})
	releaseFirst := make(chan struct{})
	var (
		mu       sync.Mutex
		attempts int
	)
	outbox := newAdvisorCgroupPostCommitOutbox(func(_ context.Context, _ advisorCgroupPostCommitEvent) error {
		mu.Lock()
		attempts++
		mu.Unlock()
		close(firstAttempt)
		<-releaseFirst
		return errors.New("controlled apply failure")
	})
	outbox.start()
	t.Cleanup(outbox.stop)

	outbox.enqueue(advisorCgroupPostCommitEvent{revision: 3, token: 23})
	<-firstAttempt
	advanceDone := make(chan struct{})
	go func() {
		outbox.advance(4)
		close(advanceDone)
	}()
	close(releaseFirst)
	<-advanceDone

	require.Eventually(t, func() bool {
		outbox.mu.Lock()
		defer outbox.mu.Unlock()
		return len(outbox.pending) == 0
	}, time.Second, time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, attempts, "a response older than any committed target must not retry")
}

func TestAdvisorCgroupOutboxCommitWaitsForWholeInFlightApply(t *testing.T) {
	firstWrite := make(chan struct{})
	releaseApply := make(chan struct{})
	commitWaiting := make(chan struct{})
	commitDone := make(chan struct{})
	var (
		releaseOnce sync.Once
		mu          sync.Mutex
		order       []string
	)

	outbox := newAdvisorCgroupPostCommitOutbox(func(_ context.Context, _ advisorCgroupPostCommitEvent) error {
		mu.Lock()
		order = append(order, "old-first-write")
		mu.Unlock()
		close(firstWrite)
		<-releaseApply
		mu.Lock()
		order = append(order, "old-second-write")
		mu.Unlock()
		return nil
	})
	outbox.start()
	t.Cleanup(outbox.stop)
	t.Cleanup(func() {
		releaseOnce.Do(func() {
			close(releaseApply)
		})
	})

	outbox.enqueue(advisorCgroupPostCommitEvent{revision: 3, token: 23})
	<-firstWrite
	var commitWaitOnce sync.Once
	outbox.beforeSequenceLock = func() {
		commitWaitOnce.Do(func() {
			close(commitWaiting)
		})
	}

	go func() {
		_ = outbox.linearizeTargetCommit(context.Background(), func() (uint64, error) {
			mu.Lock()
			order = append(order, "new-target-commit")
			mu.Unlock()
			return 4, nil
		})
		close(commitDone)
	}()
	<-commitWaiting

	releaseOnce.Do(func() {
		close(releaseApply)
	})
	require.Eventually(t, func() bool {
		select {
		case <-commitDone:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"old-first-write", "old-second-write", "new-target-commit"}, order)
}

func TestAdvisorCgroupOutboxStopSkipsApplyWaitingBehindCommit(t *testing.T) {
	commitEntered := make(chan struct{})
	releaseCommit := make(chan struct{})
	commitDone := make(chan struct{})
	workerWaiting := make(chan struct{})
	sendCalled := make(chan struct{})

	outbox := newAdvisorCgroupPostCommitOutbox(func(_ context.Context, _ advisorCgroupPostCommitEvent) error {
		close(sendCalled)
		return nil
	})
	outbox.start()

	go func() {
		_ = outbox.linearizeTargetCommit(context.Background(), func() (uint64, error) {
			close(commitEntered)
			<-releaseCommit
			return 4, nil
		})
		close(commitDone)
	}()
	<-commitEntered

	var workerWaitOnce sync.Once
	outbox.beforeSequenceLock = func() {
		workerWaitOnce.Do(func() {
			close(workerWaiting)
		})
	}
	outbox.enqueue(advisorCgroupPostCommitEvent{revision: 4, token: 44})
	<-workerWaiting
	stopDone := make(chan struct{})
	go func() {
		outbox.stop()
		close(stopDone)
	}()
	require.Eventually(t, func() bool {
		outbox.mu.Lock()
		defer outbox.mu.Unlock()
		return !outbox.started
	}, time.Second, time.Millisecond)

	close(releaseCommit)
	<-commitDone
	require.Eventually(t, func() bool {
		select {
		case <-stopDone:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	select {
	case <-sendCalled:
		require.Fail(t, "cgroup apply started after Stop canceled a worker waiting behind commit")
	default:
	}
}

func TestAdvisorCgroupOutboxApplyDeadlineUnblocksQueuedCommit(t *testing.T) {
	applyStarted := make(chan struct{})
	commitWaiting := make(chan struct{})
	commitDone := make(chan struct{})
	applyTimeout := 20 * time.Millisecond

	outbox := newAdvisorCgroupPostCommitOutbox(func(ctx context.Context, _ advisorCgroupPostCommitEvent) error {
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.WithinDuration(t, time.Now().Add(applyTimeout), deadline, 100*time.Millisecond)
		close(applyStarted)
		<-ctx.Done()
		return ctx.Err()
	})
	outbox.applyTimeout = applyTimeout
	outbox.start()
	t.Cleanup(outbox.stop)

	outbox.enqueue(advisorCgroupPostCommitEvent{revision: 3, token: 23})
	<-applyStarted
	var commitWaitOnce sync.Once
	outbox.beforeSequenceLock = func() {
		commitWaitOnce.Do(func() {
			close(commitWaiting)
		})
	}
	go func() {
		_ = outbox.linearizeTargetCommit(context.Background(), func() (uint64, error) {
			return 4, nil
		})
		close(commitDone)
	}()
	<-commitWaiting

	require.Eventually(t, func() bool {
		select {
		case <-commitDone:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}
