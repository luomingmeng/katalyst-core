/*
Copyright 2022 The Katalyst Authors.

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

package podkiller

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	pluginapi "github.com/kubewharf/katalyst-api/pkg/protocol/evictionplugin/v1alpha1"
	"github.com/kubewharf/katalyst-core/pkg/agent/evictionmanager/rule"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
)

type fixedTargetTestKiller struct {
	target    *v1.Pod
	lookupErr error
	evicted   []*v1.Pod
}

func (f *fixedTargetTestKiller) Name() string { return "fixed-target" }
func (f *fixedTargetTestKiller) ResolveEvictionTarget(_ context.Context, target *v1.Pod) (*v1.Pod, error) {
	if f.lookupErr != nil {
		return nil, f.lookupErr
	}
	return f.target.DeepCopy(), nil
}

func (f *fixedTargetTestKiller) Evict(_ context.Context, target *v1.Pod, _ int64, _, _ string) error {
	f.evicted = append(f.evicted, target.DeepCopy())
	return nil
}

func TestQueuedEvictionRetainsTargetAndKiller(t *testing.T) {
	t.Parallel()
	target := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "target", Namespace: "default", UID: "uid"}}
	selected := &fixedTargetTestKiller{target: target.DeepCopy()}
	ordinary := &recordingRuleKiller{name: "ordinary"}
	rb, err := NewRuleBasedKiller(ordinary, []KillerRule{testKillerRule{name: "source", priority: 1, matched: true, killer: selected}})
	require.NoError(t, err)
	client := fake.NewSimpleClientset(target)
	queue := NewAsynchronizedPodKiller(rb, &pod.PodFetcherStub{}, client).(*AsynchronizedPodKiller)
	defer queue.queue.ShutDown()
	require.NoError(t, queue.EvictPod(&rule.RuledEvictPod{EvictPod: &pluginapi.EvictPod{Pod: target, Reason: "pressure", EvictionPluginName: "plugin"}}))
	rb.(*ruleBasedKiller).rules = nil
	target.Name = "caller-mutated"
	key := evictionKeyFunc(podKeyFunc("default", "target", "uid"), 0)
	err, requeue := queue.sync(key)
	require.NoError(t, err)
	require.False(t, requeue)
	require.Len(t, selected.evicted, 1)
	require.Equal(t, "target", selected.evicted[0].Name)
	require.False(t, ordinary.evicted)
	require.Empty(t, client.Actions())
}

type fixedTargetTestRule struct {
	testKillerRule
	match func(*v1.Pod) (bool, Killer)
}

func (r fixedTargetTestRule) Match(target *v1.Pod) (bool, Killer) {
	return r.match(target)
}

func TestQueuedExistingBindingSkipsReselection(t *testing.T) {
	t.Parallel()
	for _, grace := range []int64{1, 30, 60} {
		grace := grace
		t.Run(fmt.Sprint(grace), func(t *testing.T) {
			t.Parallel()
			target := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "target", Namespace: "default", UID: "uid", Annotations: map[string]string{"source": "original"}}}
			selected := &fixedTargetTestKiller{target: target.DeepCopy()}
			ordinary := &recordingRuleKiller{name: "ordinary"}
			var matches int32
			router, err := NewRuleBasedKiller(ordinary, []KillerRule{fixedTargetTestRule{
				testKillerRule: testKillerRule{name: "source"},
				match: func(target *v1.Pod) (bool, Killer) {
					atomic.AddInt32(&matches, 1)
					if target.Annotations["source"] != "original" {
						return true, nil
					}
					return true, selected
				},
			}})
			require.NoError(t, err)
			client := fake.NewSimpleClientset()
			queue := NewAsynchronizedPodKiller(router, &pod.PodFetcherStub{}, client).(*AsynchronizedPodKiller)
			defer queue.queue.ShutDown()
			rp := &rule.RuledEvictPod{EvictPod: &pluginapi.EvictPod{Pod: target, DeletionOptions: &pluginapi.DeletionOptions{GracePeriodSeconds: 30}}}
			require.NoError(t, queue.EvictPod(rp))
			// Changed projection metadata would fail routing, but must not
			// override an existing binding or block grace deduplication.
			target.Annotations["source"] = "changed"
			rp.DeletionOptions.GracePeriodSeconds = grace
			require.NoError(t, queue.EvictPod(rp))
			require.Equal(t, int32(1), atomic.LoadInt32(&matches))
			podKey := podKeyFunc("default", "target", "uid")
			executedGrace := int64(30)
			if grace < 30 {
				executedGrace = grace
				require.Len(t, queue.processingPods[podKey], 2)
			} else {
				require.Len(t, queue.processingPods[podKey], 1)
			}
			info := queue.processingPods[podKey][executedGrace]
			require.Same(t, selected, info.Killer)
			require.Equal(t, "original", info.Pod.Annotations["source"])
			err, requeue := queue.sync(evictionKeyFunc(podKey, executedGrace))
			require.NoError(t, err)
			require.False(t, requeue)
			require.Len(t, selected.evicted, 1)
			require.False(t, ordinary.evicted)
			require.Empty(t, client.Actions())
		})
	}
}

func TestQueuedBindingCommittedDuringSelectionTakesPrecedence(t *testing.T) {
	t.Parallel()
	for _, grace := range []int64{1, 30} {
		grace := grace
		t.Run(fmt.Sprint(grace), func(t *testing.T) {
			t.Parallel()
			target := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "target", Namespace: "default", UID: "uid", Annotations: map[string]string{"source": "original"}}}
			selected := &fixedTargetTestKiller{target: target.DeepCopy()}
			entered, release := make(chan struct{}), make(chan struct{})
			defer close(release)
			router, err := NewRuleBasedKiller(&recordingRuleKiller{name: "ordinary"}, []KillerRule{fixedTargetTestRule{
				testKillerRule: testKillerRule{name: "source"},
				match: func(target *v1.Pod) (bool, Killer) {
					if target.Annotations["source"] == "original" {
						return true, selected
					}
					close(entered)
					<-release
					return true, nil
				},
			}})
			require.NoError(t, err)
			queue := NewAsynchronizedPodKiller(router, &pod.PodFetcherStub{}, fake.NewSimpleClientset()).(*AsynchronizedPodKiller)
			defer queue.queue.ShutDown()
			changed := target.DeepCopy()
			changed.Annotations["source"] = "changed"
			result := make(chan error, 1)
			go func() {
				result <- queue.EvictPod(&rule.RuledEvictPod{EvictPod: &pluginapi.EvictPod{Pod: changed, DeletionOptions: &pluginapi.DeletionOptions{GracePeriodSeconds: grace}}})
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("rule selection did not start")
			}
			// A second enqueue must complete while the first rule is blocked.
			committed := make(chan error, 1)
			go func() {
				committed <- queue.EvictPod(&rule.RuledEvictPod{EvictPod: &pluginapi.EvictPod{Pod: target, DeletionOptions: &pluginapi.DeletionOptions{GracePeriodSeconds: 30}}})
			}()
			select {
			case err := <-committed:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("rule selection held the queue lock")
			}
			release <- struct{}{}
			select {
			case err := <-result:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("enqueue did not finish after rule selection")
			}
			podKey := podKeyFunc("default", "target", "uid")
			queue.RLock()
			info := queue.processingPods[podKey][grace]
			count := len(queue.processingPods[podKey])
			queue.RUnlock()
			if grace < 30 {
				require.Equal(t, 2, count)
			} else {
				require.Equal(t, 1, count)
			}
			require.NotNil(t, info)
			require.Same(t, selected, info.Killer)
			require.Equal(t, "original", info.Pod.Annotations["source"])
		})
	}
}

func TestQueuedOrdinaryPodDeduplicatesBeforeRouting(t *testing.T) {
	t.Parallel()
	target := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "target", Namespace: "default", UID: "uid"}}
	router, err := NewRuleBasedKiller(&recordingRuleKiller{name: "ordinary"}, nil)
	require.NoError(t, err)
	queue := NewAsynchronizedPodKiller(router, &pod.PodFetcherStub{}, fake.NewSimpleClientset()).(*AsynchronizedPodKiller)
	defer queue.queue.ShutDown()
	rp := &rule.RuledEvictPod{EvictPod: &pluginapi.EvictPod{Pod: target, DeletionOptions: &pluginapi.DeletionOptions{GracePeriodSeconds: 30}}}
	require.NoError(t, queue.EvictPod(rp))
	router.(*ruleBasedKiller).rules = []KillerRule{testKillerRule{name: "invalid", matched: true}}
	for _, grace := range []int64{30, 60} {
		rp.DeletionOptions.GracePeriodSeconds = grace
		require.NoError(t, queue.EvictPod(rp))
	}
	rp.DeletionOptions.GracePeriodSeconds = 1
	require.Error(t, queue.EvictPod(rp))
	items := queue.processingPods[podKeyFunc("default", "target", "uid")]
	require.Len(t, items, 1)
	require.Nil(t, items[30].Killer)
}

func TestQueuedOrdinaryPodKeepsWorkerRouting(t *testing.T) {
	t.Parallel()
	target := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "target", Namespace: "default", UID: "uid"}}
	for _, matched := range []bool{false, true} {
		matched := matched
		t.Run(fmt.Sprintf("matched=%t", matched), func(t *testing.T) {
			t.Parallel()
			initial := &recordingRuleKiller{name: "initial"}
			current := &recordingRuleKiller{name: "current"}
			router, err := NewRuleBasedKiller(initial, []KillerRule{testKillerRule{name: "source", matched: matched, killer: initial}})
			require.NoError(t, err)
			client := fake.NewSimpleClientset(target)
			queue := NewAsynchronizedPodKiller(router, &pod.PodFetcherStub{PodList: []*v1.Pod{target}}, client).(*AsynchronizedPodKiller)
			defer queue.queue.ShutDown()
			require.NoError(t, queue.EvictPod(&rule.RuledEvictPod{EvictPod: &pluginapi.EvictPod{Pod: target}}))
			router.(*ruleBasedKiller).rules = []KillerRule{testKillerRule{name: "current", matched: true, killer: current}}
			err, requeue := queue.sync(evictionKeyFunc(podKeyFunc("default", "target", "uid"), 0))
			require.NoError(t, err)
			require.False(t, requeue)
			require.False(t, initial.evicted)
			require.True(t, current.evicted)
			require.Empty(t, client.Actions())
		})
	}
}

func TestQueuedMatchedNilKillerRejectsEnqueue(t *testing.T) {
	t.Parallel()
	router, err := NewRuleBasedKiller(&recordingRuleKiller{name: "default"}, []KillerRule{testKillerRule{name: "invalid", matched: true}})
	require.NoError(t, err)
	queue := NewAsynchronizedPodKiller(router, &pod.PodFetcherStub{}, fake.NewSimpleClientset()).(*AsynchronizedPodKiller)
	defer queue.queue.ShutDown()
	target := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "target", Namespace: "default", UID: "uid"}}
	require.Error(t, queue.EvictPod(&rule.RuledEvictPod{EvictPod: &pluginapi.EvictPod{Pod: target}}))
	require.Empty(t, queue.processingPods)
}

func TestQueuedFixedSourceDoesNotFallback(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"lookup failure", "changed uid", "changed name", "changed namespace"} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			target := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "target", Namespace: "default", UID: "uid"}}
			selected := &fixedTargetTestKiller{target: target.DeepCopy()}
			switch mode {
			case "lookup failure":
				selected.lookupErr = fmt.Errorf("source unavailable")
			case "changed uid":
				selected.target.UID = "new"
			case "changed name":
				selected.target.Name = "new"
			case "changed namespace":
				selected.target.Namespace = "new"
			}
			client := fake.NewSimpleClientset(target)
			queue := NewAsynchronizedPodKiller(selected, &pod.PodFetcherStub{PodList: []*v1.Pod{target}}, client).(*AsynchronizedPodKiller)
			defer queue.queue.ShutDown()
			require.NoError(t, queue.EvictPod(&rule.RuledEvictPod{EvictPod: &pluginapi.EvictPod{Pod: target}}))
			err, requeue := queue.sync(evictionKeyFunc(podKeyFunc("default", "target", "uid"), 0))
			require.Error(t, err)
			require.True(t, requeue)
			require.Empty(t, selected.evicted)
			require.Empty(t, client.Actions())
		})
	}
}
