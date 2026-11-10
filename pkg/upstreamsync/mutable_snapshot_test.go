// Copyright The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package upstreamsync_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	v1 "k8s.io/api/core/v1"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/backend/cache"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	"sigs.k8s.io/scheduler-library/pkg/upstreamsync"
	testutils "sigs.k8s.io/scheduler-library/pkg/upstreamsync/testutils"
)

func scheduledPodsMap(t *testing.T, snap *cache.Snapshot, ns, pgName string) map[string]string {
	t.Helper()
	pgState, err := snap.PodGroupStates().Get(ns, pgName)
	if err != nil {
		t.Fatalf("PodGroupStates().Get(%s/%s) unexpected error: %v", ns, pgName, err)
	}
	got := make(map[string]string, len(pgState.ScheduledPods()))
	for _, p := range pgState.ScheduledPods() {
		got[p.Name] = p.Spec.NodeName
	}
	if pgState.ScheduledPodsCount() != len(got) {
		t.Errorf("ScheduledPodsCount() = %d, want %d", pgState.ScheduledPodsCount(), len(got))
	}
	return got
}

func TestMutatingSnapshot_PodGroupStates(t *testing.T) {
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.GenericWorkload: true,
	})

	t.Run("RemovePod and AddPod maintain PodGroupStates for assigned and assumed pods and preserve node generation", func(t *testing.T) {
		logger := klog.FromContext(t.Context())
		node1 := st.MakeNode().Name("node1").Capacity(map[v1.ResourceName]string{
			v1.ResourceCPU:    "8",
			v1.ResourceMemory: "8Gi",
			v1.ResourcePods:   "10",
		}).Obj()
		pg := testutils.MakeGangPodGroup("gang-pg", "", 2)

		assignedPod := testutils.MakePod("assigned-pod", "gang-pg", "2")
		assignedPod.Spec.NodeName = "node1"
		unscheduledPod := testutils.MakePod("assumed-pod", "gang-pg", "2")

		snap := cache.NewTestSnapshotWithPodGroups([]*v1.Pod{assignedPod, unscheduledPod}, []*v1.Node{node1}, []*schedulingv1beta1.PodGroup{pg})

		assumedCopy := unscheduledPod.DeepCopy()
		assumedCopy.Spec.NodeName = "node1"
		assumedPI, err := framework.NewPodInfo(assumedCopy)
		if err != nil {
			t.Fatalf("NewPodInfo(assumedCopy) failed: %v", err)
		}
		if err := snap.AssumePod(assumedPI); err != nil {
			t.Fatalf("AssumePod failed: %v", err)
		}

		niBefore, err := snap.NodeInfos().Get("node1")
		if err != nil {
			t.Fatalf("NodeInfos().Get(node1) failed: %v", err)
		}
		genBefore := niBefore.GetGeneration()

		ms := upstreamsync.NewMutatingSnapshot(snap)
		assignedPI, err := framework.NewPodInfo(assignedPod)
		if err != nil {
			t.Fatalf("NewPodInfo(assignedPod) failed: %v", err)
		}

		// Remove assigned pod.
		if err := ms.RemovePod(logger, assignedPI); err != nil {
			t.Fatalf("RemovePod(assignedPI) failed: %v", err)
		}
		if diff := cmp.Diff(map[string]string{"assumed-pod": "node1"}, scheduledPodsMap(t, snap, "default", "gang-pg")); diff != "" {
			t.Errorf("ScheduledPods after removing assigned-pod (-want +got):\n%s", diff)
		}

		// Remove assumed pod.
		if err := ms.RemovePod(logger, assumedPI); err != nil {
			t.Fatalf("RemovePod(assumedPI) failed: %v", err)
		}
		if diff := cmp.Diff(map[string]string{}, scheduledPodsMap(t, snap, "default", "gang-pg"), cmpopts.EquateEmpty()); diff != "" {
			t.Errorf("ScheduledPods after removing assumed-pod (-want +got):\n%s", diff)
		}

		// RestoreState puts both pods back in their exact original categories (assigned vs assumed),
		// so ForgetPod still transitions assumed-pod back to UnscheduledPods.
		ms.RestoreState()
		if diff := cmp.Diff(map[string]string{
			"assigned-pod": "node1",
			"assumed-pod":  "node1",
		}, scheduledPodsMap(t, snap, "default", "gang-pg")); diff != "" {
			t.Errorf("ScheduledPods after RestoreState (-want +got):\n%s", diff)
		}
		pgState, err := snap.PodGroupStates().Get("default", "gang-pg")
		if err != nil {
			t.Fatalf("PodGroupStates().Get failed: %v", err)
		}
		if !pgState.AssumedPods().Equal(sets.New[types.UID](assumedCopy.UID)) {
			t.Errorf("AssumedPods after RestoreState = %v, want {%s}", pgState.AssumedPods().UnsortedList(), assumedCopy.UID)
		}

		if err := snap.ForgetPod(logger, assumedCopy); err != nil {
			t.Fatalf("ForgetPod(assumedCopy) after RestoreState failed: %v", err)
		}
		if diff := cmp.Diff(map[string]string{"assigned-pod": "node1"}, scheduledPodsMap(t, snap, "default", "gang-pg")); diff != "" {
			t.Errorf("ScheduledPods after ForgetPod (-want +got):\n%s", diff)
		}

		niAfter, err := snap.NodeInfos().Get("node1")
		if err != nil {
			t.Fatalf("NodeInfos().Get(node1) failed: %v", err)
		}
		if niAfter.GetGeneration() != genBefore {
			t.Errorf("node1 generation = %d, want %d", niAfter.GetGeneration(), genBefore)
		}
	})

	t.Run("AddPod and RemovePod handle pod-only PodGroupState lifecycle cleanly", func(t *testing.T) {
		logger := klog.FromContext(t.Context())
		node1 := st.MakeNode().Name("node1").Capacity(map[v1.ResourceName]string{
			v1.ResourceCPU:    "4",
			v1.ResourceMemory: "8Gi",
			v1.ResourcePods:   "10",
		}).Obj()

		snap := cache.NewSnapshot(nil, []*v1.Node{node1})
		ms := upstreamsync.NewMutatingSnapshot(snap)

		newPod := testutils.MakePod("added-pg-pod", "orphan-pg", "1")
		newPod.Spec.NodeName = "node1"
		newPI, err := framework.NewPodInfo(newPod)
		if err != nil {
			t.Fatalf("NewPodInfo failed: %v", err)
		}

		if err := ms.AddPod(logger, newPI); err != nil {
			t.Fatalf("AddPod failed: %v", err)
		}
		if diff := cmp.Diff(map[string]string{"added-pg-pod": "node1"}, scheduledPodsMap(t, snap, "default", "orphan-pg")); diff != "" {
			t.Errorf("ScheduledPods after AddPod (-want +got):\n%s", diff)
		}

		ms.RestoreState()
		if _, err := snap.PodGroupStates().Get("default", "orphan-pg"); err == nil {
			t.Errorf("expected orphan-pg PodGroupState to be deleted when last pod is reverted and PodGroup object is nil")
		}
	})
}
