// +kubebuilder:object:generate=true
// +groupName=infrastructure.tutorial.example.com

// Package v1alpha1 defines the first, deliberately small GPUCluster API.
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var GroupVersion = schema.GroupVersion{Group: "infrastructure.tutorial.example.com", Version: "v1alpha1"}

func AddToScheme(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion, &GPUCluster{}, &GPUClusterList{})
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}

// GPUClusterSpec is immutable until a later lesson adds a spec-change policy.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec changes are not supported in this lesson"
type GPUClusterSpec struct {
	// NodeCount requests allocations, without claiming the hosts are healthy.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	NodeCount int32 `json:"nodeCount"`
	// ImageVersion is the simulated operating system image intent.
	// +kubebuilder:default="os-v1"
	// +kubebuilder:validation:MinLength=1
	// +optional
	ImageVersion string `json:"imageVersion,omitempty"`
	// GPUClass is validated by the provider so unsupported classes demonstrate
	// terminal workflow failure rather than API admission failure.
	// +kubebuilder:default="mock-a100"
	// +kubebuilder:validation:MinLength=1
	// +optional
	GPUClass string `json:"gpuClass,omitempty"`
	// MaxParallel bounds per-host provisioning within one workflow.
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10
	// +optional
	MaxParallel int32 `json:"maxParallel,omitempty"`
}

type AllocatedHost struct {
	ID   string `json:"id"`
	Slot int32  `json:"slot"`
}

type GPUClusterStatus struct {
	ObservedGeneration int64           `json:"observedGeneration,omitempty"`
	AllocatedHosts     int32           `json:"allocatedHosts,omitempty"`
	Hosts              []AllocatedHost `json:"hosts,omitempty"`
	WorkflowID         string          `json:"workflowID,omitempty"`
	RunID              string          `json:"runID,omitempty"`
	// Phase reports the durable operation; Ready also requires observed inventory.
	Phase      string `json:"phase,omitempty"`
	ReadyHosts int32  `json:"readyHosts,omitempty"`
	LastError  string `json:"lastError,omitempty"`
	// Conditions describe allocation confirmation separately from last-known capacity.
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=gpuc
type GPUCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              GPUClusterSpec   `json:"spec"`
	Status            GPUClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type GPUClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []GPUCluster `json:"items"`
}
