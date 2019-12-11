/*
Copyright 2019 The KubeMacPool Authors.

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

package pool_manager

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"

	multus "github.com/intel/multus-cni/types"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (p *PoolManager) AllocatePodMac(pod *corev1.Pod) error {
	p.poolMutex.Lock()
	defer p.poolMutex.Unlock()

	log.V(1).Info("AllocatePodMac: Data",
		"macmap", p.macPoolMap,
		"podmap", p.podToMacPoolMap,
		"currentMac", p.currentMac.String())

	networkValue, ok := pod.Annotations[networksAnnotation]
	if !ok {
		return nil
	}

	// allocate only when the network status is no exist
	// we want to connect the allocated mac from the webhook to a pod object in the podToMacPoolMap
	// run it before multus added the status annotation
	// this mean the pod is not ready
	if _, ok := pod.Annotations[networksStatusAnnotation]; ok {
		return nil
	}

	networks, err := parsePodNetworkAnnotation(networkValue, pod.Namespace)
	if err != nil {
		return err
	}

	log.V(1).Info("pod meta data", "podMetaData", (*pod).ObjectMeta)

	if len(networks) == 0 {
		return nil
	}

	// validate if the pod is related to kubevirt
	if p.isRelatedToKubevirt(pod) {
		// nothing to do here. the mac is already by allocated by the virtual machine webhook
		log.V(1).Info("This pod have ownerReferences from kubevirt skipping")
		return nil
	}

	allocations := []string{}
	networkList := []*multus.NetworkSelectionElement{}
	for _, network := range networks {
		if network.MacRequest != "" {
			if err := p.allocatePodRequestedMac(network, pod); err != nil {
				p.revertAllocationOnPodMac(podNamespaced(pod), allocations)
				return err
			}
			allocations = append(allocations, network.MacRequest)
		} else {
			macAddr, err := p.allocatePodFromMacPool(network, pod)
			if err != nil {
				p.revertAllocationOnPodMac(podNamespaced(pod), allocations)
				return err
			}

			network.MacRequest = macAddr
			allocations = append(allocations, macAddr)
		}
		networkList = append(networkList, network)
	}

	networkListJson, err := json.Marshal(networkList)
	if err != nil {
		return err
	}
	pod.Annotations[networksAnnotation] = string(networkListJson)

	return nil
}

func (p *PoolManager) AllocatePodGUID(pod *corev1.Pod) error {
	p.poolMutex.Lock()
	defer p.poolMutex.Unlock()

	log.V(1).Info("AllocatePodGUID: Data",
		"guidmap", p.macPoolMap,
		"podmap", p.podToGUIDPoolMap,
		"currentGUID", p.currentGUID.String())

	networkValue, ok := pod.Annotations[networksAnnotation]
	if !ok {
		return nil
	}

	// allocate only when the network status is no exist
	// we want to connect the allocated guid from the webhook to a pod object in the podToGUIDPoolMap
	// run it before multus added the status annotation
	// this mean the pod is not ready
	if _, ok := pod.Annotations[networksStatusAnnotation]; ok {
		return nil
	}

	networks, err := parsePodNetworkAnnotation(networkValue, pod.Namespace)
	if err != nil {
		return err
	}

	log.V(1).Info("pod meta data", "podMetaData", (*pod).ObjectMeta)

	if len(networks) == 0 {
		return nil
	}

	// validate if the pod is related to kubevirt
	if p.isRelatedToKubevirt(pod) {
		// nothing to do here. the guid is not supported for virtual machine
		log.V(1).Info("This pod have ownerReferences from kubevirt skipping, guid not supported for virtual machines")
		return nil
	}

	allocations := []string{}
	networkList := []*multus.NetworkSelectionElement{}
	for _, network := range networks {
		if network.GUIDRequest != "" {
			if err := p.allocatePodRequestedGUID(network, pod); err != nil {
				p.revertAllocationOnPodGUID(podNamespaced(pod), allocations)
				return err
			}
			allocations = append(allocations, network.GUIDRequest)
		} else {
			guidAddr, err := p.allocatePodFromGUIDPool(network, pod)
			if err != nil {
				p.revertAllocationOnPodGUID(podNamespaced(pod), allocations)
				return err
			}

			network.GUIDRequest = guidAddr
			allocations = append(allocations, guidAddr)
		}
		networkList = append(networkList, network)
	}

	networkListJson, err := json.Marshal(networkList)
	if err != nil {
		return err
	}
	pod.Annotations[networksAnnotation] = string(networkListJson)

	return nil
}

func (p *PoolManager) ReleasePodMac(podName string) error {
	p.poolMutex.Lock()
	defer p.poolMutex.Unlock()

	log.V(1).Info("ReleasePodMac: Data",
		"macmap", p.macPoolMap,
		"podmap", p.podToMacPoolMap,
		"currentMac", p.currentMac.String())

	macList, ok := p.podToMacPoolMap[podName]

	if !ok {
		log.Error(fmt.Errorf("not found"), "pod not found in the map",
			"podName", podName)
		return nil
	}

	if macList == nil {
		log.Error(fmt.Errorf("list empty"), "failed to get mac address list")
		return nil
	}

	for _, macAddr := range macList {
		delete(p.macPoolMap, macAddr)
		log.Info("released mac from pod", "mac", macAddr, "pod", podName)
	}

	delete(p.podToMacPoolMap, podName)
	log.V(1).Info("removed pod from podToMacPoolMap", "pod", podName)
	return nil
}

func (p *PoolManager) ReleasePodGUID(podName string) error {
	p.poolMutex.Lock()
	defer p.poolMutex.Unlock()

	log.V(1).Info("ReleasePodGUID: Data",
		"guidmap", p.guidPoolMap,
		"podmap", p.podToGUIDPoolMap,
		"currentGUID", p.currentGUID.String())

	guidList, ok := p.podToGUIDPoolMap[podName]

	if !ok {
		log.Error(fmt.Errorf("not found"), "pod not found in the map",
			"podName", podName)
		return nil
	}

	if guidList == nil {
		log.Error(fmt.Errorf("list empty"), "failed to get guid address list")
		return nil
	}

	for _, guidAddr := range guidList {
		delete(p.guidPoolMap, guidAddr)
		log.Info("released guid from pod", "guid", guidAddr, "pod", podName)
	}

	delete(p.podToGUIDPoolMap, podName)
	log.V(1).Info("removed pod from podToGUIDPoolMap", "pod", podName)
	return nil
}

func (p *PoolManager) allocatePodRequestedMac(network *multus.NetworkSelectionElement, pod *corev1.Pod) error {
	requestedMac := network.MacRequest
	if _, err := net.ParseMAC(requestedMac); err != nil {
		return err
	}

	if macAllocationStatus, exist := p.macPoolMap[requestedMac]; exist &&
		macAllocationStatus == AllocationStatusAllocated &&
		!p.allocatedToCurrentPodMac(podNamespaced(pod), network) {

		err := fmt.Errorf("failed to allocate requested mac address")
		log.Error(err, "mac address already allocated")

		return err
	}

	if pod.Name == "" {
		// we are going to create the podToMacPoolMap in the controller call
		// because we don't have the pod name in the webhook
		p.macPoolMap[requestedMac] = AllocationStatusWaitingForPod
		return nil
	}

	p.macPoolMap[requestedMac] = AllocationStatusAllocated
	if p.podToMacPoolMap[podNamespaced(pod)] == nil {
		p.podToMacPoolMap[podNamespaced(pod)] = map[string]string{}
	}
	p.podToMacPoolMap[podNamespaced(pod)][network.Name] = requestedMac
	log.Info("requested mac was allocated for pod",
		"requestedMap", requestedMac,
		"podName", pod.Name,
		"podNamespace", pod.Namespace)

	return nil
}

func (p *PoolManager) allocatePodRequestedGUID(network *multus.NetworkSelectionElement, pod *corev1.Pod) error {
	requestedGUID := network.GUIDRequest
	if _, err := net.ParseMAC(requestedGUID); err != nil {
		return err
	}

	if guidAllocationStatus, exist := p.guidPoolMap[requestedGUID]; exist &&
		guidAllocationStatus == AllocationStatusAllocated &&
		!p.allocatedToCurrentPodGUID(podNamespaced(pod), network) {

		err := fmt.Errorf("failed to allocate requested guid address")
		log.Error(err, "guid address already allocated")

		return err
	}

	if pod.Name == "" {
		// we are going to create the podToGuidPoolMap in the controller call
		// because we don't have the pod name in the webhook
		p.guidPoolMap[requestedGUID] = AllocationStatusWaitingForPod
		return nil
	}

	p.guidPoolMap[requestedGUID] = AllocationStatusAllocated
	if p.podToGUIDPoolMap[podNamespaced(pod)] == nil {
		p.podToGUIDPoolMap[podNamespaced(pod)] = map[string]string{}
	}
	p.podToGUIDPoolMap[podNamespaced(pod)][network.Name] = requestedGUID
	log.Info("requested guid was allocated for pod",
		"requestedMap", requestedGUID,
		"podName", pod.Name,
		"podNamespace", pod.Namespace)

	return nil
}

func (p *PoolManager) allocatePodFromMacPool(network *multus.NetworkSelectionElement, pod *corev1.Pod) (string, error) {
	macAddr, err := p.getFreeMac()
	if err != nil {
		return "", err
	}

	if pod.Name == "" {
		// we are going to create the podToMacPoolMap in the controller call
		// because we don't have the pod name in the webhook
		p.macPoolMap[macAddr.String()] = AllocationStatusWaitingForPod
		return macAddr.String(), nil
	}

	p.macPoolMap[macAddr.String()] = AllocationStatusAllocated
	if p.podToMacPoolMap[podNamespaced(pod)] == nil {
		p.podToMacPoolMap[podNamespaced(pod)] = map[string]string{}
	}
	p.podToMacPoolMap[podNamespaced(pod)][network.Name] = macAddr.String()
	log.Info("mac from pool was allocated to the pod",
		"allocatedMac", macAddr.String(),
		"podName", pod.Name,
		"podNamespace", pod.Namespace)
	return macAddr.String(), nil
}

func (p *PoolManager) allocatePodFromGUIDPool(network *multus.NetworkSelectionElement, pod *corev1.Pod) (string, error) {
	guidAddr, err := p.getFreeGUID()
	if err != nil {
		return "", err
	}

	if pod.Name == "" {
		// we are going to create the podToGUIDPoolMap in the controller call
		// because we don't have the pod name in the webhook
		p.guidPoolMap[guidAddr.String()] = AllocationStatusWaitingForPod
		return guidAddr.String(), nil
	}

	p.guidPoolMap[guidAddr.String()] = AllocationStatusAllocated
	if p.podToGUIDPoolMap[podNamespaced(pod)] == nil {
		p.podToGUIDPoolMap[podNamespaced(pod)] = map[string]string{}
	}
	p.podToGUIDPoolMap[podNamespaced(pod)][network.Name] = guidAddr.String()
	log.Info("guid from pool was allocated to the pod",
		"allocatedGUID", guidAddr.String(),
		"podName", pod.Name,
		"podNamespace", pod.Namespace)
	return guidAddr.String(), nil
}

func (p *PoolManager) initPodMap() error {
	log.V(1).Info("start InitMaps to reserve existing mac addresses before allocation new ones")
	pods, err := p.kubeClient.CoreV1().Pods("").List(metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, pod := range pods.Items {
		log.V(1).Info("InitMaps for pod", "podName", pod.Name, "podNamespace", pod.Namespace)
		if pod.Annotations == nil {
			continue
		}

		networkValue, ok := pod.Annotations[networksAnnotation]
		if !ok {
			continue
		}

		networks, err := parsePodNetworkAnnotation(networkValue, pod.Namespace)
		if err != nil {
			continue
		}

		log.V(1).Info("pod meta data", "podMetaData", pod.ObjectMeta)
		if len(networks) == 0 {
			continue
		}

		// validate if the pod is related to kubevirt
		if p.isRelatedToKubevirt(&pod) {
			// nothing to do here. the mac is already by allocated by the virtual machine webhook
			log.V(1).Info("This pod have ownerReferences from kubevirt skipping")
			continue
		}

		for _, network := range networks {
			if network.MacRequest != "" {
				if err := p.allocatePodRequestedMac(network, &pod); err != nil {
					// Dont return an error here if we can't allocate a mac for a running pod
					log.Error(fmt.Errorf("failed to parse mac address for pod"),
						"Invalid mac address for pod",
						"namespace", pod.Namespace,
						"name", pod.Name,
						"mac", network.MacRequest)
				}
			}
			if network.GUIDRequest != "" {
				if err := p.allocatePodRequestedGUID(network, &pod); err != nil {
					// Dont return an error here if we can't allocate a mac for a running pod
					log.Error(fmt.Errorf("failed to parse mac address for pod"),
						"Invalid guid address for pod",
						"namespace", pod.Namespace,
						"name", pod.Name,
						"guid", network.GUIDRequest)
				}
			}
		}
	}

	return nil
}

func (p *PoolManager) allocatedToCurrentPodMac(podname string, network *multus.NetworkSelectionElement) bool {
	networks, exist := p.podToMacPoolMap[podname]
	if !exist {
		return false
	}

	allocatedMac, exist := networks[network.Name]

	if !exist {
		return false
	}

	if allocatedMac == network.MacRequest {
		return true
	}

	return false
}

func (p *PoolManager) allocatedToCurrentPodGUID(podname string, network *multus.NetworkSelectionElement) bool {
	networks, exist := p.podToGUIDPoolMap[podname]
	if !exist {
		return false
	}

	allocatedGUID, exist := networks[network.Name]

	if !exist {
		return false
	}

	if allocatedGUID == network.GUIDRequest {
		return true
	}

	return false
}

// Revert allocation if one of the requested mac addresses fails to be allocated
func (p *PoolManager) revertAllocationOnPodMac(podName string, allocations []string) {
	log.V(1).Info("Rever vm allocation", "podName", podName, "allocations", allocations)
	for _, value := range allocations {
		delete(p.macPoolMap, value)
	}
	delete(p.podToMacPoolMap, podName)
}

// Revert allocation if one of the requested guid addresses fails to be allocated
func (p *PoolManager) revertAllocationOnPodGUID(podName string, allocations []string) {
	log.V(1).Info("Revert vm allocation", "podName", podName, "allocations", allocations)
	for _, value := range allocations {
		delete(p.guidPoolMap, value)
	}
	delete(p.podToGUIDPoolMap, podName)
}

func podNamespaced(pod *corev1.Pod) string {
	return fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
}

func parsePodNetworkAnnotation(podNetworks, defaultNamespace string) ([]*multus.NetworkSelectionElement, error) {
	var networks []*multus.NetworkSelectionElement

	if podNetworks == "" {
		return nil, fmt.Errorf("parsePodNetworkAnnotation: pod annotation not having \"network\" as key, refer Multus README.md for the usage guide")
	}

	if strings.IndexAny(podNetworks, "[{\"") >= 0 {
		if err := json.Unmarshal([]byte(podNetworks), &networks); err != nil {
			return nil, fmt.Errorf("parsePodNetworkAnnotation: failed to parse pod Network Attachment Selection Annotation JSON format: %v", err)
		}
	} else {
		log.Info("Only JSON List Format for networks is allowed to be parsed")
		return networks, nil
	}

	for _, network := range networks {
		if network.Namespace == "" {
			network.Namespace = defaultNamespace
		}
	}

	return networks, nil
}
