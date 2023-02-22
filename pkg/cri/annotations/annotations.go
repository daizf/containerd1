/*
   Copyright The containerd Authors.

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

package annotations

import runtime "k8s.io/cri-api/pkg/apis/runtime/v1"

// ContainerType values
// Following OCI annotations are used by katacontainers now.
// We'll switch to standard secure pod API after it is defined in CRI.
const (
	// ContainerTypeSandbox represents a pod sandbox container
	ContainerTypeSandbox = "sandbox"

	// ContainerTypeContainer represents a container running within a pod
	ContainerTypeContainer = "container"

	// ContainerType is the container type (sandbox or container) annotation
	ContainerType = "io.kubernetes.cri.container-type"

	// SandboxID is the sandbox ID annotation
	SandboxID = "io.kubernetes.cri.sandbox-id"

	// SandboxCPU annotations are based on the initial CPU configuration for the sandbox. This is calculated as the
	// sum of container CPU resources, optionally provided by Kubelet (introduced  in 1.23) as part of the PodSandboxConfig
	SandboxCPUPeriod = "io.kubernetes.cri.sandbox-cpu-period"
	SandboxCPUQuota  = "io.kubernetes.cri.sandbox-cpu-quota"
	SandboxCPUShares = "io.kubernetes.cri.sandbox-cpu-shares"

	// SandboxMemory is the initial amount of memory associated with this sandbox. This is calculated as the sum
	// of container memory, optionally provided by Kubelet (introduced in 1.23) as part of the PodSandboxConfig.
	SandboxMem = "io.kubernetes.cri.sandbox-memory"

	// SandboxLogDir is the pod log directory annotation.
	// If the sandbox needs to generate any log, it will put it into this directory.
	// Kubelet will be responsible for:
	// 1) Monitoring the disk usage of the log, and including it as part of the pod
	// ephemeral storage usage.
	// 2) Cleaning up the logs when the pod is deleted.
	// NOTE: Kubelet is not responsible for rotating the logs.
	SandboxLogDir = "io.kubernetes.cri.sandbox-log-directory"

	// UntrustedWorkload is the sandbox annotation for untrusted workload. Untrusted
	// workload can only run on dedicated runtime for untrusted workload.
	UntrustedWorkload = "io.kubernetes.cri.untrusted-workload"

	// SandboxNamespace is the name of the namespace of the sandbox (pod)
	SandboxNamespace = "io.kubernetes.cri.sandbox-namespace"

	// SandboxName is the name of the sandbox (pod)
	SandboxName = "io.kubernetes.cri.sandbox-name"

	// ContainerName is the name of the container in the pod
	ContainerName = "io.kubernetes.cri.container-name"

	// ImageName is the name of the image used to create the container
	ImageName = "io.kubernetes.cri.image-name"

	// PodAnnotations are the annotations of the pod
	PodAnnotations = "io.kubernetes.cri.pod-annotations"

	// WindowsHostProcess is used by hcsshim to identify windows pods that are running HostProcesses
	WindowsHostProcess = "microsoft.com/hostprocess-container"

	// OSFeature is the feature that pod will active
	OSFeature = "io.containerd.osfeature"

	// ImageUser tells runtime the username to use. According to OCI image
	// spec, this could be in the following format:
	// "user,uid,user:group,uid:gid,uid:group,user:gid"
	ImageUser = "io.containerd.image.user"

	// ImageGids tells runtime the additional gids to the given uid or
	// username.
	ImageGids = "io.containerd.image.gids"

	// NydusHint tells containerd this pod need nydus remote image
	NydusHint = "nydus.remoteimage.v1"

	// DadiHint tells containerd this pod need dadi remote image
	DadiHint = "dadi.remoteimage.v1"
)

// HasImageAccHint check if the nydus or dadi hint annotation is passed
func HasImageAccHint(config *runtime.PodSandboxConfig) (string, bool) {
	if config == nil || config.Annotations == nil {
		return "", false
	}

	sandboxAnnotations := config.Annotations
	if feature, ok := sandboxAnnotations[OSFeature]; ok {
		if feature == NydusHint {
			return "nydus", true
		}
		if feature == DadiHint {
			return "dadi", true
		}
	}

	return "", false
}
