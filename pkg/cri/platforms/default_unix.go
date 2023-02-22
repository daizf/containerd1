// +build !windows

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

package platforms

import (
	"github.com/containerd/containerd/pkg/cri/snapshotter"
	v1 "k8s.io/cri-api/pkg/apis/runtime/v1"

	criconfig "github.com/containerd/containerd/pkg/cri/config"

	"github.com/containerd/containerd/pkg/cri/annotations"
	"github.com/containerd/containerd/platforms"
)

// Default returns the current platform's default platform specification.
func Default() platforms.MatchComparer {
	return platforms.Default()
}

// GetPlatformFromPodAnnotation get platform matcher from pod annotation and criconfig
// if pod created with nydus hint, prefer nydus snapshotter otherwise, we will check if
// containerd start with nydus snapshotter, we should use nydus first matcher if containerd
// start with nydus snapshotter
func GetPlatformFromPodAnnotation(config criconfig.Config, podSandboxConfig *v1.PodSandboxConfig) platforms.MatchComparer {
	if podSandboxConfig != nil && podSandboxConfig.Annotations != nil {
		if feature, ok := podSandboxConfig.Annotations[annotations.OSFeature]; ok {
			if feature == annotations.NydusHint {
				return platforms.DefaultComparerWithOSFeatures()
			}
		}
	}
	return defaultWithNydusSnapshotter(config)
}

func defaultWithNydusSnapshotter(config criconfig.Config) platforms.MatchComparer {
	if config.Snapshotter == snapshotter.NydusSnapshotter {
		return platforms.DefaultComparerWithOSFeatures()
	}
	return platforms.Default()
}
