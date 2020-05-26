package framework

import (
	"encoding/json"
	"fmt"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog"

	columbiav1 "github.com/columbia/sage/privacyresource/pkg/apis/columbia.github.io/v1"
)

type PodHandler struct {
	pod  *v1.Pod
	lock sync.RWMutex
}

func NewPodHandler(pod *v1.Pod) *PodHandler {
	return &PodHandler{pod: pod}
}

func (handler *PodHandler) Snapshot() *v1.Pod {
	handler.lock.RLock()
	defer handler.lock.RUnlock()
	if handler.pod == nil {
		return nil
	}
	return handler.pod.DeepCopy()
}

func (handler *PodHandler) Update(pod *v1.Pod) error {
	handler.lock.Lock()
	defer handler.lock.Unlock()
	if pod.Generation < handler.pod.Generation {
		return fmt.Errorf("pod [%v] to update is staled", pod.Name)
	}
	handler.pod = pod
	return nil
}

func (handler *PodHandler) Delete() {
	handler.lock.Lock()
	defer handler.lock.Unlock()
	handler.pod = nil
}

func (handler *PodHandler) IsLive() bool {
	handler.lock.RLock()
	defer handler.lock.RUnlock()
	return handler.pod != nil
}

func (handler *PodHandler) GetBlocks() ([]string, error) {
	handler.lock.RLock()
	defer handler.lock.RUnlock()
	var res []string

	if handler.pod.Annotations == nil {
		return res, nil
	}
	blockArrayStr, ok := handler.pod.Annotations["dp-blocks"]
	if !ok {
		return res, nil
	}
	if err := json.Unmarshal([]byte(blockArrayStr), &res); err != nil {
		klog.Errorf("Unable to decode dp-blocks for pod %s", handler.pod.Name)
		return nil, err
	}
	return res, nil
}

func (handler *PodHandler) GetName() string {
	handler.lock.RLock()
	defer handler.lock.RUnlock()
	if handler.pod == nil {
		return ""
	}
	return handler.pod.Name
}

type BlockHandler struct {
	block *columbiav1.PrivateDataBlock
	lock  sync.RWMutex
}

func NewBlockHandler(block *columbiav1.PrivateDataBlock) *BlockHandler {
	return &BlockHandler{block: block}
}

func (handler *BlockHandler) Snapshot() *columbiav1.PrivateDataBlock {
	handler.lock.RLock()
	defer handler.lock.RUnlock()
	if handler.block == nil {
		return nil
	}
	return handler.block.DeepCopy()
}

func (handler *BlockHandler) Update(block *columbiav1.PrivateDataBlock) error {
	handler.lock.Lock()
	defer handler.lock.Unlock()
	if block.Generation < handler.block.Generation {
		return fmt.Errorf("block [%v] to update is staled", block.Name)
	}
	handler.block = block
	return nil
}

func (handler *BlockHandler) Delete() {
	handler.lock.Lock()
	defer handler.lock.Unlock()
	handler.block = nil
}

func (handler *BlockHandler) IsLive() bool {
	handler.lock.RLock()
	defer handler.lock.RUnlock()
	return handler.block == nil
}
