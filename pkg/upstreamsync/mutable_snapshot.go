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

package upstreamsync

import (
	"fmt"
	"reflect"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/backend/cache"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

/*

This is a proposal for exposing Add/RemovePod function on the snapshot.

This is needed because snapshot tracks internal state such as podGroupStates
(accessed here via reflection because podGroupStates is unexported on cache.Snapshot),
which must be updated together with the deletion or addition of the pod.

Snapshot already exposes AssumePod/ForgetPod, but it's specifically for pods that were not originally in the snapshot, which is useful in scheduling (i.e. "Assume" and then undo with "Forget").
We will want to expose AddPod/RemovePod for pods that were originally in the snapshot, which is useful in preemption (i.e. "Remove" and then undo with "Add").

Both cases can be unified under the same Add/Remove operations.

UPSTREAM-DIFF: the whole file is library-only. It has no counterpart in kube-scheduler yet; it
lives here because it is proposed for the upstream snapshot, not because it was copied from it.
When upstream cache.Snapshot exposes reversible AddPod/RemovePod methods that maintain
podGroupStates directly, replace MutatingSnapshot (including its reflection helpers) with those
upstream methods.

*/

type removedPodGroupMember struct {
	pgKey          fwk.EntityKey
	pgGeneration   int64
	uid            types.UID
	allPod         *v1.Pod
	wasUnscheduled bool
	assumedPod     *v1.Pod
	wasAssumed     bool
	wasAssigned    bool
}

// MutatingSnapshot wraps a cache.Snapshot with Add/RemovePod operations that keep track of what
// was changed, so that the snapshot can be restored to the state it had when the wrapper was
// created (see RestoreState).
type MutatingSnapshot struct {
	*cache.Snapshot
	addedPods              map[string]func()
	removedPods            map[string]func()
	addedPodGroupMembers   map[string]func()
	removedPodGroupMembers map[string]*removedPodGroupMember
}

// NewMutatingSnapshot wraps a cache.Snapshot with reversible AddPod and RemovePod operations.
func NewMutatingSnapshot(snapshot *cache.Snapshot) *MutatingSnapshot {
	return &MutatingSnapshot{
		Snapshot:               snapshot,
		addedPods:              map[string]func(){},
		removedPods:            map[string]func(){},
		addedPodGroupMembers:   map[string]func(){},
		removedPodGroupMembers: map[string]*removedPodGroupMember{},
	}
}

// RemovePod removes a pod from its assigned node and PodGroupState in the snapshot without advancing generation.
func (s *MutatingSnapshot) RemovePod(logger klog.Logger, podInfo *framework.PodInfo) error {
	key, err := framework.GetPodKey(podInfo.Pod)
	if err != nil {
		return err
	}

	ni, err := s.GetNodeByPod(podInfo.Pod)
	if err != nil {
		return err
	}

	fni, ok := ni.(*framework.NodeInfo)
	if !ok {
		return fmt.Errorf("unknown node info type: %T", ni)
	}
	oldGen := fni.Generation
	defer func() {
		fni.Generation = oldGen
	}()
	err = ni.RemovePod(logger, podInfo.Pod)
	if err != nil {
		return err
	}

	if undoPG, ok := s.addedPodGroupMembers[key]; ok {
		undoPG()
		delete(s.addedPodGroupMembers, key)
	} else if rec := s.removeFromPodGroupState(podInfo.Pod); rec != nil {
		s.removedPodGroupMembers[key] = rec
	}

	if _, ok := s.addedPods[key]; ok {
		delete(s.addedPods, key)
	} else {
		s.removedPods[key] = func() { _ = s.AddPod(logger, podInfo) }
	}
	return nil
}

// AddPod adds a pod to its assigned node and PodGroupState in the snapshot without advancing generation.
func (s *MutatingSnapshot) AddPod(logger klog.Logger, podInfo *framework.PodInfo) error {
	key, err := framework.GetPodKey(podInfo.Pod)
	if err != nil {
		return err
	}

	ni, err := s.GetNodeByPod(podInfo.Pod)
	if err != nil {
		return err
	}

	fni, ok := ni.(*framework.NodeInfo)
	if !ok {
		return fmt.Errorf("unknown node info type: %T", ni)
	}
	oldGen := fni.Generation
	defer func() {
		fni.Generation = oldGen
	}()
	ni.AddPodInfo(podInfo)

	if rec, ok := s.removedPodGroupMembers[key]; ok {
		s.restorePodGroupMember(rec, podInfo.Pod.Spec.NodeName)
		delete(s.removedPodGroupMembers, key)
	} else if undoPG := s.addToPodGroupState(podInfo.Pod); undoPG != nil {
		s.addedPodGroupMembers[key] = undoPG
	}

	if _, ok := s.removedPods[key]; ok {
		delete(s.removedPods, key)
	} else {
		s.addedPods[key] = func() { _ = s.RemovePod(logger, podInfo) }
	}
	return nil
}

// RestoreState reverts all pod additions and removals recorded on the MutatingSnapshot.
func (s *MutatingSnapshot) RestoreState() {
	for _, restoreFn := range s.addedPods {
		restoreFn()
	}
	for _, restoreFn := range s.removedPods {
		restoreFn()
	}

	s.addedPods = map[string]func(){}
	s.removedPods = map[string]func(){}
	s.addedPodGroupMembers = map[string]func(){}
	s.removedPodGroupMembers = map[string]*removedPodGroupMember{}
}

// GetNodeByPod returns the NodeInfo for the node specified in pod.Spec.NodeName.
func (s *MutatingSnapshot) GetNodeByPod(pod *v1.Pod) (fwk.NodeInfo, error) {
	nodeName := pod.Spec.NodeName
	ni, err := s.Get(nodeName)
	if err != nil {
		return nil, err
	}
	return ni, nil
}

func isPodGroupMember(pod *v1.Pod) bool {
	return pod != nil &&
		pod.Spec.SchedulingGroup != nil &&
		pod.Spec.SchedulingGroup.PodGroupName != nil &&
		*pod.Spec.SchedulingGroup.PodGroupName != ""
}

func writableField(v reflect.Value, name string) reflect.Value {
	f := v.FieldByName(name)
	return reflect.NewAt(f.Type(), f.Addr().UnsafePointer()).Elem()
}

func getOrCreatePodGroupState(rv reflect.Value, key fwk.EntityKey) (pgs reflect.Value, created bool) {
	pgsMap := writableField(rv, "podGroupStates")
	if pgsMap.IsNil() {
		pgsMap.Set(reflect.MakeMap(pgsMap.Type()))
	}
	keyVal := reflect.ValueOf(key)
	pgs = pgsMap.MapIndex(keyVal)
	if !pgs.IsValid() {
		pgs = reflect.New(pgsMap.Type().Elem().Elem())
		for _, field := range []string{"allPods", "unscheduledPods", "assumedPods", "assignedPods"} {
			f := writableField(pgs.Elem(), field)
			f.Set(reflect.MakeMap(f.Type()))
		}
		pgsMap.SetMapIndex(keyVal, pgs)
		created = true
	}
	return pgs, created
}

func podGroupStateEmpty(pgs reflect.Value) bool {
	return pgs.Elem().FieldByName("podGroup").IsNil() &&
		pgs.Elem().FieldByName("allPods").Len() == 0
}

func (s *MutatingSnapshot) removeFromPodGroupState(pod *v1.Pod) *removedPodGroupMember {
	if !isPodGroupMember(pod) {
		return nil
	}
	rv := reflect.ValueOf(s.Snapshot).Elem()
	if !rv.FieldByName("genericWorkloadEnabled").Bool() {
		return nil
	}
	pgsMap := writableField(rv, "podGroupStates")
	if pgsMap.IsNil() {
		return nil
	}
	pgKey := fwk.PodGroupKey(pod.Namespace, *pod.Spec.SchedulingGroup.PodGroupName)
	pgKeyVal := reflect.ValueOf(pgKey)
	pgs := pgsMap.MapIndex(pgKeyVal)
	if !pgs.IsValid() {
		return nil
	}

	allPods := writableField(pgs.Elem(), "allPods").Interface().(map[types.UID]*v1.Pod)
	allPod, exists := allPods[pod.UID]
	if !exists {
		return nil
	}
	unscheduledPods := writableField(pgs.Elem(), "unscheduledPods").Interface().(sets.Set[types.UID])
	assumedPods := writableField(pgs.Elem(), "assumedPods").Interface().(map[types.UID]*v1.Pod)
	assignedPods := writableField(pgs.Elem(), "assignedPods").Interface().(sets.Set[types.UID])
	assumedPod, wasAssumed := assumedPods[pod.UID]

	rec := &removedPodGroupMember{
		pgKey:          pgKey,
		pgGeneration:   writableField(pgs.Elem(), "generation").Int(),
		uid:            pod.UID,
		allPod:         allPod,
		wasUnscheduled: unscheduledPods.Has(pod.UID),
		assumedPod:     assumedPod,
		wasAssumed:     wasAssumed,
		wasAssigned:    assignedPods.Has(pod.UID),
	}

	delete(allPods, pod.UID)
	unscheduledPods.Delete(pod.UID)
	delete(assumedPods, pod.UID)
	assignedPods.Delete(pod.UID)
	if podGroupStateEmpty(pgs) {
		pgsMap.SetMapIndex(pgKeyVal, reflect.Value{})
	}
	return rec
}

func (s *MutatingSnapshot) restorePodGroupMember(rec *removedPodGroupMember, nodeName string) {
	rv := reflect.ValueOf(s.Snapshot).Elem()
	if !rv.FieldByName("genericWorkloadEnabled").Bool() {
		return
	}
	pgs, created := getOrCreatePodGroupState(rv, rec.pgKey)
	if created {
		writableField(pgs.Elem(), "generation").SetInt(rec.pgGeneration)
	}

	allPod := rec.allPod
	if rec.wasAssigned && allPod != nil && allPod.Spec.NodeName != nodeName {
		allPod = allPod.DeepCopy()
		allPod.Spec.NodeName = nodeName
	}
	writableField(pgs.Elem(), "allPods").Interface().(map[types.UID]*v1.Pod)[rec.uid] = allPod
	if rec.wasUnscheduled {
		writableField(pgs.Elem(), "unscheduledPods").Interface().(sets.Set[types.UID]).Insert(rec.uid)
	}
	if rec.wasAssumed {
		assumedPod := rec.assumedPod
		if assumedPod != nil && assumedPod.Spec.NodeName != nodeName {
			assumedPod = assumedPod.DeepCopy()
			assumedPod.Spec.NodeName = nodeName
		}
		writableField(pgs.Elem(), "assumedPods").Interface().(map[types.UID]*v1.Pod)[rec.uid] = assumedPod
	}
	if rec.wasAssigned {
		writableField(pgs.Elem(), "assignedPods").Interface().(sets.Set[types.UID]).Insert(rec.uid)
	}
}

func (s *MutatingSnapshot) addToPodGroupState(pod *v1.Pod) func() {
	if !isPodGroupMember(pod) {
		return nil
	}
	rv := reflect.ValueOf(s.Snapshot).Elem()
	if !rv.FieldByName("genericWorkloadEnabled").Bool() {
		return nil
	}
	pgKey := fwk.PodGroupKey(pod.Namespace, *pod.Spec.SchedulingGroup.PodGroupName)
	pgs, _ := getOrCreatePodGroupState(rv, pgKey)
	allPods := writableField(pgs.Elem(), "allPods").Interface().(map[types.UID]*v1.Pod)
	if _, exists := allPods[pod.UID]; exists {
		return nil
	}

	storedPod := pod.DeepCopy()
	allPods[pod.UID] = storedPod
	writableField(pgs.Elem(), "assignedPods").Interface().(sets.Set[types.UID]).Insert(pod.UID)

	uid := pod.UID
	return func() {
		snapRV := reflect.ValueOf(s.Snapshot).Elem()
		pgsMap := writableField(snapRV, "podGroupStates")
		if pgsMap.IsNil() {
			return
		}
		keyVal := reflect.ValueOf(pgKey)
		currPGS := pgsMap.MapIndex(keyVal)
		if !currPGS.IsValid() {
			return
		}
		delete(writableField(currPGS.Elem(), "allPods").Interface().(map[types.UID]*v1.Pod), uid)
		writableField(currPGS.Elem(), "unscheduledPods").Interface().(sets.Set[types.UID]).Delete(uid)
		delete(writableField(currPGS.Elem(), "assumedPods").Interface().(map[types.UID]*v1.Pod), uid)
		writableField(currPGS.Elem(), "assignedPods").Interface().(sets.Set[types.UID]).Delete(uid)
		if podGroupStateEmpty(currPGS) {
			pgsMap.SetMapIndex(keyVal, reflect.Value{})
		}
	}
}
