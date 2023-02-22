// +build linux

package diskquota

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/containerd/containerd/snapshots"
	"github.com/pkg/errors"
)

// CheckRegularFile is used to check the file is regular file or directory.
func CheckRegularFile(file string) (bool, error) {
	fd, err := os.Lstat(file)
	if err != nil {
		return false, err
	}

	if fd.Mode()&(os.ModeSymlink|os.ModeNamedPipe|os.ModeSocket|os.ModeDevice) == 0 {
		return true, nil
	}

	return false, nil
}

// loadQuotaIDs loads quota IDs for quota driver from reqquota execution result.
// This function utils `repquota` which summarizes quotas for a filesystem.
// see http://man7.org/linux/man-pages/man8/repquota.8.html
//
// $ repquota -Pan
// Project         used    soft    hard  grace    used  soft  hard  grace
// ----------------------------------------------------------------------
// #0        --     220       0       0             25     0     0
// #123      --       4       0 88589934592          1     0     0
// #8888     --       8       0       0              2     0     0
func loadQuotaIDs(repquotaOpt string) (map[uint32]struct{}, uint32, error) {
	quotaIDs := make(map[uint32]struct{})

	minID := QuotaMinID
	output, stderr, err := snapshots.ExecSync("repquota", repquotaOpt)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "failed to execute [repquota %s], stdout: (%s), stderr: (%s)",
			repquotaOpt, output, stderr)
	}

	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		if len(line) == 0 || line[0] != '#' {
			continue
		}
		// find all lines with prefix '#'
		parts := strings.Split(line, " ")
		// part[0] is "#123456"
		if len(parts[0]) <= 1 {
			continue
		}

		id, err := strconv.Atoi(parts[0][1:])
		quotaID := uint32(id)
		if err == nil && quotaID > QuotaMinID {
			quotaIDs[quotaID] = struct{}{}
			if quotaID > minID {
				minID = quotaID
			}
		}
	}
	return quotaIDs, minID, nil
}

// getDevLimit returns the device storage upper limit.
func getDevLimit(mountPoint string) (uint64, error) {
	// get storage upper limit of the device which the dir is on.
	var stfs syscall.Statfs_t
	if err := syscall.Statfs(mountPoint, &stfs); err != nil {
		return 0, errors.Wrapf(err, "failed to get path(%s) limit", mountPoint)
	}
	return stfs.Blocks * uint64(stfs.Bsize), nil
}

// checkDevLimit checks if the device on which the input dir lies has already been recorded in driver.
func checkDevLimit(mountPoint string, size uint64) error {
	limit, err := getDevLimit(mountPoint)
	if err != nil {
		return errors.Wrapf(err, "failed to get device(%s) limit", mountPoint)
	}

	if limit < size {
		return fmt.Errorf("dir %s quota limit %v must be less than %v", mountPoint, size, limit)
	}
	return nil
}
