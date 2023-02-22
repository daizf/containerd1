package snapshots

import (
	"bytes"
	"os/exec"
)

const (
	// add label type=image in Snapshotter metadata Info.Labels to indicate the snapshot for image.
	TypeLabelKey  = "containerd.io/snapshot/type"
	ImageType     = "image"
	ContainerType = "container"

	// SnapshotLabelContextKey is grpc key which allows user transfers snapshot label by context
	SnapshotLabelContextKey = "containerd.io.snapshot.labels"
)

func ExecSync(bin string, args ...string) (std, serr string, err error) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := exec.Command(bin, args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()
	if err != nil {
		return stdout.String(), stderr.String(), err
	}
	return stdout.String(), stderr.String(), nil
}
