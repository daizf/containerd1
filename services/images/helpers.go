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

package images

import (
	imagesapi "github.com/containerd/containerd/api/services/images/v1"
	"github.com/containerd/containerd/api/types"
	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/platforms"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	// remoteImageLabel is a label which contains remote image type
	// and will be saved in image store.
	remoteImageLabel = "containerd.io/image/platform.osfeature"

	// nydusHint is a value for remoteImageLabel which is originally
	// taken out from image manifest->platform->osfeatures and should
	// saved in image store.
	nydusHint = "nydus.remoteimage.v1"
)

func imagesToProto(images []images.Image) []imagesapi.Image {
	var imagespb []imagesapi.Image

	for _, image := range images {
		imagespb = append(imagespb, imageToProto(&image))
	}

	return imagespb
}

func imageToProto(image *images.Image) imagesapi.Image {
	return imagesapi.Image{
		Name:      image.Name,
		Labels:    saveOSFeatureInLabels(image.Target.Platform, image.Labels),
		Target:    descToProto(&image.Target),
		CreatedAt: image.CreatedAt,
		UpdatedAt: image.UpdatedAt,
	}
}

func imageFromProto(imagepb *imagesapi.Image) images.Image {
	return images.Image{
		Name:      imagepb.Name,
		Labels:    imagepb.Labels,
		Target:    descFromProto(&imagepb.Target, imagepb.Labels),
		CreatedAt: imagepb.CreatedAt,
		UpdatedAt: imagepb.UpdatedAt,
	}
}

func saveOSFeatureInLabels(platform *ocispec.Platform, labels map[string]string) map[string]string {
	if platform == nil {
		return labels
	}

	for _, v := range platform.OSFeatures {
		if v == nydusHint {
			if labels == nil {
				labels = make(map[string]string)
			}
			labels[remoteImageLabel] = nydusHint

			return labels
		}
	}
	return labels
}

func descFromProto(desc *types.Descriptor, labels map[string]string) ocispec.Descriptor {
	return ocispec.Descriptor{
		MediaType:   desc.MediaType,
		Size:        desc.Size_,
		Digest:      desc.Digest,
		Annotations: desc.Annotations,
		Platform:    platformFromProto(labels),
	}
}

func descToProto(desc *ocispec.Descriptor) types.Descriptor {
	return types.Descriptor{
		MediaType:   desc.MediaType,
		Size_:       desc.Size,
		Digest:      desc.Digest,
		Annotations: desc.Annotations,
	}
}

func platformFromProto(labels map[string]string) *ocispec.Platform {
	var platform ocispec.Platform

	if feature, ok := labels[remoteImageLabel]; ok {
		if feature == nydusHint {
			platform = platforms.DefaultSpecWithOSFeatures()
		}
	}

	platform = platforms.DefaultSpec()
	return &platform
}
