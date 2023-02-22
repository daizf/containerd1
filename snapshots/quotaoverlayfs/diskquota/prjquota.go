//go:build linux
// +build linux

package diskquota

import (
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/docker/go-units"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
)

const (
	// QuotaMinID represents the minimum quota id.
	// The value is unit32(2^24).
	QuotaMinID = uint32(16777216)

	// QuotaMaxID represents the maximum quota id.
	QuotaMaxID = uint32(200000000)
)

// SetDiskQuotaBytes set dir project quota to the quotaId
func SetDiskQuotaBytes(dir string, limit int64, quotaID uint32) error {
	driver := &PrjQuotaDriver{}
	mountPoint, hasQuota, err := driver.CheckMountpoint(dir)
	if err != nil {
		return err
	}
	if !hasQuota {
		// no need to remount option prjquota for mountpoint
		return fmt.Errorf("mountpoint: (%s) not enable prjquota", mountPoint)
	}

	if err := checkDevLimit(mountPoint, uint64(limit)); err != nil {
		return errors.Wrapf(err, "failed to check device limit, dir: (%s), limit: (%d)kb", dir, limit)
	}

	err = driver.SetFileAttr(dir, quotaID)
	if err != nil {
		return errors.Wrapf(err, "failed to set subtree, dir: (%s), quota id: (%d)", dir, quotaID)
	}

	return driver.setQuota(quotaID, uint64(limit/1024), mountPoint)
}

// PrjQuotaDriver represents project quota driver.
type PrjQuotaDriver struct {
	lock sync.Mutex

	// quotaIDs saves all of quota ids.
	// key: quota ID which means this ID is used in the global scope.
	// value: stuct{}
	QuotaIDs map[uint32]struct{}

	// lastID is used to mark last used quota ID.
	// quota ID is allocated increasingly by sequence one by one.
	LastID uint32
}

// SetDiskQuota uses the following two parameters to set disk quota for a directory.
// * quota size: a byte size of requested quota.
// * quota ID: an ID represent quota attr which is used in the global scope.
func (quota *PrjQuotaDriver) SetDiskQuota(dir string, size string, quotaID uint32) error {
	mountPoint, hasQuota, err := quota.CheckMountpoint(dir)
	if err != nil {
		return err
	}
	if !hasQuota {
		// no need to remount option prjquota for mountpoint
		return fmt.Errorf("mountpoint: (%s) not enable prjquota", mountPoint)
	}

	limit, err := units.RAMInBytes(size)
	if err != nil {
		return errors.Wrapf(err, "failed to change size: (%s) to kilobytes", size)
	}

	if err := checkDevLimit(mountPoint, uint64(limit)); err != nil {
		return errors.Wrapf(err, "failed to check device limit, dir: (%s), limit: (%d)kb", dir, limit)
	}

	err = quota.SetFileAttr(dir, quotaID)
	if err != nil {
		return errors.Wrapf(err, "failed to set subtree, dir: (%s), quota id: (%d)", dir, quotaID)
	}

	return quota.setQuota(quotaID, uint64(limit/1024), mountPoint)
}

func (quota *PrjQuotaDriver) CheckMountpoint(dir string) (string, bool, error) {
	mountInfo, err := mount.Lookup(dir)
	if err != nil {
		return "", false, errors.Wrapf(err, "failed to get mount info, dir(%s)", dir)
	}
	if strings.Contains(mountInfo.VFSOptions, "prjquota") {
		return mountInfo.Mountpoint, true, nil
	}
	return mountInfo.Mountpoint, false, nil
}

// setQuota uses system tool "setquota" to set project quota for binding of limit and mountpoint and quotaID.
// * quotaID: quota ID which means this ID is used in the global scope.
// * blockLimit: block limit number for mountpoint.
// * mountPoint: the mountpoint of the device in the filesystem
// ext4: setquota -P qid $softlimit $hardlimit $softinode $hardinode mountpoint
func (quota *PrjQuotaDriver) setQuota(quotaID uint32, blockLimit uint64, mountPoint string) error {
	quotaIDStr := strconv.FormatUint(uint64(quotaID), 10)
	blockLimitStr := strconv.FormatUint(blockLimit, 10)

	// ext4 set project quota limit
	stdout, stderr, err := snapshots.ExecSync("setquota", "-P", quotaIDStr, "0", blockLimitStr, "0", "0", mountPoint)
	if err != nil {
		return errors.Wrapf(err, "failed to set quota, mountpoint: (%s), quota id: (%d), quota: (%d kbytes), stdout: (%s), stderr: (%s)",
			mountPoint, quotaID, blockLimit, stdout, stderr)
	}
	return nil
}

// GetQuotaIDInFileAttr gets attributes of the file which is in the inode.
// The returned result is quota ID.
// return 0 if failure happens, since quota ID must be positive.
// execution command: `lsattr -p $dir`
func (quota *PrjQuotaDriver) GetQuotaIDInFileAttr(dir string) uint32 {
	parent := path.Dir(dir)
	qid := 0

	stdout, _, err := snapshots.ExecSync("lsattr", "-p", parent)
	if err != nil {
		// failure, then return invalid value 0 for quota ID.
		return 0
	}

	// example output:
	// 16777256 --------------e---P ./exampleDir
	lines := strings.Split(stdout, "\n")
	for _, line := range lines {
		parts := strings.Split(line, " ")
		if len(parts) > 2 && parts[2] == dir {
			// find the corresponding quota ID, return directly.
			qid, _ = strconv.Atoi(parts[0])
			return uint32(qid)
		}
	}

	return 0
}

// GetNextQuotaID returns the next available quota id.
func (quota *PrjQuotaDriver) GetNextQuotaID() (quotaID uint32, err error) {
	quota.lock.Lock()
	defer quota.lock.Unlock()

	if quota.LastID == 0 {
		quota.QuotaIDs, quota.LastID, err = loadQuotaIDs("-Pan")
		if err != nil {
			return 0, errors.Wrap(err, "failed to load quota list")
		}
	}
	id := quota.LastID
	for {
		if id < QuotaMinID {
			id = QuotaMinID
		}
		id++
		if _, ok := quota.QuotaIDs[id]; !ok {
			if id <= QuotaMaxID {
				break
			}
			logrus.Infof("reach the maximum, try to reuse quotaID")
			quota.QuotaIDs, quota.LastID, err = loadQuotaIDs("-Pan")
			if err != nil {
				return 0, errors.Wrap(err, "failed to load quota list")
			}
			id = quota.LastID
		}
	}
	quota.QuotaIDs[id] = struct{}{}
	quota.LastID = id

	return id, nil
}

// SetFileAttr set the file attr.
// ext4: chattr -p quotaid +P $DIR
func (quota *PrjQuotaDriver) SetFileAttr(dir string, quotaID uint32) error {
	strID := strconv.FormatUint(uint64(quotaID), 10)

	// ext4 use chattr to change project id
	stdout, stderr, err := snapshots.ExecSync("chattr", "-p", strID, "+P", dir)
	if err != nil {
		return errors.Wrapf(err, "failed to set file(%s) quota id(%s), stdout: (%s), stderr: (%s)",
			dir, strID, stdout, stderr)
	}
	logrus.Debugf("set quota id (%s) to file (%s) attr", strID, dir)

	return nil
}
