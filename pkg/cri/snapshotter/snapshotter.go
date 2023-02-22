package snapshotter

import (
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/pkg/cri/annotations"
	"golang.org/x/net/context"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type (
	snapshotterKey struct{}
)

func Key() snapshotterKey {
	return snapshotterKey{}
}

const (
	NydusSnapshotter          = "nydus"
	DadiSnapshotter           = "dadi"
	QuotaoverlayfsSnapshotter = "quotaoverlayfs"
	AnnotationQuotaoverlayfs  = "k8s.aliyun.com/eci-rootfs-size"
)

func WithSnapshotter(ctx context.Context, snapshotter string) context.Context {
	log.L.Infof("save snapshotter %s to ctx", snapshotter)

	return context.WithValue(ctx, snapshotterKey{}, snapshotter)
}

// priority: quotaoverlayfs > nydus
func ChooseSnapshotter(ctx context.Context, config *runtime.PodSandboxConfig) context.Context {
	if config == nil {
		return ctx
	}

	var s string
	if snapshot, hasImageAcc := annotations.HasImageAccHint(config); hasImageAcc {
		s = snapshot
	}

	if config.Annotations != nil {
		if quota, ok := config.Annotations[AnnotationQuotaoverlayfs]; ok && quota != "" {
			s = QuotaoverlayfsSnapshotter
		}
	}

	log.L.Infof("save snapshotter %s to ctx", s)
	return context.WithValue(ctx, snapshotterKey{}, s)
}

func GetSnapshotter(ctx context.Context, defaultSnapshotter string) string {
	ss := ctx.Value(snapshotterKey{})
	if ss == nil {
		return defaultSnapshotter
	}
	log.L.Infof("get snapshotter %s from ctx", ss)
	return ss.(string)
}
