//go:build linux
// +build linux

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

package quotaoverlayfs

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/platforms"
	"github.com/containerd/containerd/plugin"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/snapshots/quotaoverlayfs/diskquota"
	"github.com/containerd/containerd/snapshots/storage"
	"github.com/containerd/continuity/fs"
	"github.com/pkg/errors"
)

const (
	// RootfsQuotaSnapshotterLabel sets container rootfs diskquota
	// works for sae case, this used in snapshotter implement
	RootfsQuotaSnapshotterLabel = "containerd.io/snapshot/disk_quota"
)

func init() {
	config := defaultConfig()
	plugin.Register(&plugin.Registration{
		Type:   plugin.SnapshotPlugin,
		ID:     "quotaoverlayfs",
		Config: &config,
		InitFn: func(ic *plugin.InitContext) (interface{}, error) {
			ic.Meta.Platforms = append(ic.Meta.Platforms, platforms.DefaultSpec())
			ic.Meta.Exports["root"] = ic.Root
			config, ok := ic.Config.(*SnapshotterConfig)
			if !ok {
				return nil, errors.New("invalid quotaoverlayfs configuration")
			}
			ic.Meta.Exports["EnableDiskquota"] = strconv.FormatBool(config.EnableDiskquota)
			ic.Meta.Exports["DiskQuotaSize"] = config.DiskQuotaSize
			// ignore user config
			config.AsyncRemove = true
			return NewSnapshotter(ic.Root, config)
		},
	})
}

type snapshotter struct {
	root        string
	ms          *storage.MetaStore
	config      SnapshotterConfig
	quotaDriver *diskquota.PrjQuotaDriver
}

// NewSnapshotter returns a Snapshotter which uses overlayfs. The overlayfs
// diffs are stored under the provided root. A metadata file is stored under
// the root.
func NewSnapshotter(root string, config *SnapshotterConfig) (snapshots.Snapshotter, error) {
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	supportsDType, err := fs.SupportsDType(root)
	if err != nil {
		return nil, err
	}
	if !supportsDType {
		return nil, fmt.Errorf("%s does not support d_type. If the backing filesystem is xfs, please reformat with ftype=1 to enable d_type support", root)
	}
	ms, err := storage.NewMetaStore(filepath.Join(root, "metadata.db"))
	if err != nil {
		return nil, err
	}

	if err := os.Mkdir(filepath.Join(root, "snapshots"), 0700); err != nil && !os.IsExist(err) {
		return nil, err
	}

	return &snapshotter{
		root:   root,
		ms:     ms,
		config: *config,
		quotaDriver: &diskquota.PrjQuotaDriver{
			QuotaIDs: make(map[uint32]struct{}),
		},
	}, nil
}

// Stat returns the info for an active or committed snapshot by name or
// key.
//
// Should be used for parent resolution, existence checks and to discern
// the kind of snapshot.
func (o *snapshotter) Stat(ctx context.Context, key string) (snapshots.Info, error) {
	ctx, t, err := o.ms.TransactionContext(ctx, false)
	if err != nil {
		return snapshots.Info{}, err
	}
	defer t.Rollback()
	_, info, _, err := storage.GetInfo(ctx, key)
	if err != nil {
		return snapshots.Info{}, err
	}

	return info, nil
}

func (o *snapshotter) Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (snapshots.Info, error) {
	ctx, t, err := o.ms.TransactionContext(ctx, true)
	if err != nil {
		return snapshots.Info{}, err
	}

	info, err = storage.UpdateInfo(ctx, info, fieldpaths...)
	if err != nil {
		t.Rollback()
		return snapshots.Info{}, err
	}

	if err := t.Commit(); err != nil {
		return snapshots.Info{}, err
	}

	return info, nil
}

// Usage returns the resources taken by the snapshot identified by key.
//
// For active snapshots, this will scan the usage of the overlay "diff" (aka
// "upper") directory and may take some time.
//
// For committed snapshots, the value is returned from the metadata database.
func (o *snapshotter) Usage(ctx context.Context, key string) (snapshots.Usage, error) {
	ctx, t, err := o.ms.TransactionContext(ctx, false)
	if err != nil {
		return snapshots.Usage{}, err
	}
	id, info, usage, err := storage.GetInfo(ctx, key)
	t.Rollback() // transaction no longer needed at this point.

	if err != nil {
		return snapshots.Usage{}, err
	}

	upperPath := o.upperPath(id)

	if info.Kind == snapshots.KindActive {
		du, err := fs.DiskUsage(ctx, upperPath)
		if err != nil {
			// TODO(stevvooe): Consider not reporting an error in this case.
			return snapshots.Usage{}, err
		}

		usage = snapshots.Usage(du)
	}

	return usage, nil
}

func (o *snapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return o.createSnapshot(ctx, snapshots.KindActive, key, parent, opts...)
}

func (o *snapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return o.createSnapshot(ctx, snapshots.KindView, key, parent, opts...)
}

// Mounts returns the mounts for the transaction identified by key. Can be
// called on an read-write or readonly transaction.
//
// This can be used to recover mounts after calling View or Prepare.
func (o *snapshotter) Mounts(ctx context.Context, key string) ([]mount.Mount, error) {
	ctx, t, err := o.ms.TransactionContext(ctx, false)
	if err != nil {
		return nil, err
	}
	s, err := storage.GetSnapshot(ctx, key)
	t.Rollback()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get active mount")
	}
	return o.mounts(s), nil
}

func (o *snapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	ctx, t, err := o.ms.TransactionContext(ctx, true)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			if rerr := t.Rollback(); rerr != nil {
				log.G(ctx).WithError(rerr).Warn("failed to rollback transaction")
			}
		}
	}()

	// grab the existing id
	id, _, _, err := storage.GetInfo(ctx, key)
	if err != nil {
		return err
	}

	usage, err := fs.DiskUsage(ctx, o.upperPath(id))
	if err != nil {
		return err
	}

	if _, err = storage.CommitActive(ctx, key, name, snapshots.Usage(usage), opts...); err != nil {
		return errors.Wrap(err, "failed to commit snapshot")
	}
	return t.Commit()
}

// Remove abandons the snapshot identified by key. The snapshot will
// immediately become unavailable and unrecoverable. Disk space will
// be freed up on the next call to `Cleanup`.
func (o *snapshotter) Remove(ctx context.Context, key string) (err error) {
	ctx, t, err := o.ms.TransactionContext(ctx, true)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			if rerr := t.Rollback(); rerr != nil {
				log.G(ctx).WithError(rerr).Warn("failed to rollback transaction")
			}
		}
	}()

	_, _, err = storage.Remove(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to remove")
	}

	if !o.config.AsyncRemove {
		var removals []string
		removals, err = o.getCleanupDirectories(ctx, t)
		if err != nil {
			return errors.Wrap(err, "unable to get directories for removal")
		}

		// Remove directories after the transaction is closed, failures must not
		// return error since the transaction is committed with the removal
		// key no longer available.
		defer func() {
			if err == nil {
				for _, dir := range removals {
					if err := os.RemoveAll(dir); err != nil {
						log.G(ctx).WithError(err).WithField("path", dir).Warn("failed to remove directory")
					}
				}
			}
		}()

	}

	return t.Commit()
}

// Walk the committed snapshots.
//func (o *snapshotter) Walk(ctx context.Context, fn func(context.Context, snapshots.Info) error, filters ...string) error {
func (o *snapshotter) Walk(ctx context.Context, fn snapshots.WalkFunc, filters ...string) error {
	ctx, t, err := o.ms.TransactionContext(ctx, false)
	if err != nil {
		return err
	}
	defer t.Rollback()
	return storage.WalkInfo(ctx, fn)
}

// Cleanup cleans up disk resources from removed or abandoned snapshots
func (o *snapshotter) Cleanup(ctx context.Context) error {
	cleanup, err := o.cleanupDirectories(ctx)
	if err != nil {
		return err
	}

	snapshotDir := filepath.Join(o.root, "snapshots")
	for _, dir := range cleanup {
		err := os.RemoveAll(dir)
		if err == nil {
			continue
		}

		log.G(ctx).WithError(err).WithField("path", dir).Warn("failed to remove directory")

		// NOTE: check whether it is permission error
		// If so, we should change chattr or swapoff it.
		// And next round of gc will remove it.
		if !os.IsPermission(err) {
			continue
		}

		perr, ok := err.(*os.PathError)
		if !ok {
			log.G(ctx).WithField("path", dir).Warn("failed to retry cleanup because we don't know which file has permission error")
			continue
		}

		// FIXME(yuge.fw): cleanupDirectories must return
		// the dirs in the snapshotDir. I think we can
		// skip this check.
		if matched := strings.HasPrefix(perr.Path, snapshotDir); !matched {
			log.G(ctx).WithField("path", dir).Warnf("failed to retry cleanup: %s is not sub dir/file in overlay snapshotter dir(%s)", perr.Path, snapshotDir)
			continue
		}

		// try to remove immutable flag
		cmd := exec.Command("chattr", "-i", "-a", perr.Path)
		if _, cherr := cmd.CombinedOutput(); cherr != nil {
			log.G(ctx).WithError(cherr).WithField("path", dir).Warnf("failed to chattr -i %s", perr.Path)
		}

		// try to swapoff
		cmd = exec.Command("swapoff", perr.Path)
		if _, serr := cmd.CombinedOutput(); serr != nil {
			log.G(ctx).WithError(serr).WithField("path", dir).Warnf("failed to swapoff %s", perr.Path)
		}

		// NOTE: it might still fail because there are many
		// immutable files in the rm-* dir or other unknown
		// cases.
		//
		// for the many-immutable-files case, the Cleanup will
		// be called in next snapshotter gc round. We don't
		// need to resolve the issue in one round.
		if aerr := os.RemoveAll(dir); aerr != nil {
			log.G(ctx).WithError(aerr).WithField("path", dir).Warn("failed to remove directory again")
			continue
		}
		log.G(ctx).WithField("path", dir).Info("try to resolve the permission error and remove it successfully")
	}
	return nil
}

func (o *snapshotter) cleanupDirectories(ctx context.Context) ([]string, error) {
	// Get a write transaction to ensure no other write transaction can be entered
	// while the cleanup is scanning.
	ctx, t, err := o.ms.TransactionContext(ctx, true)
	if err != nil {
		return nil, err
	}

	defer t.Rollback()
	return o.getCleanupDirectories(ctx, t)
}

func (o *snapshotter) getCleanupDirectories(ctx context.Context, t storage.Transactor) ([]string, error) {
	ids, err := storage.IDMap(ctx)
	if err != nil {
		return nil, err
	}

	snapshotDir := filepath.Join(o.root, "snapshots")
	fd, err := os.Open(snapshotDir)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	dirs, err := fd.Readdirnames(0)
	if err != nil {
		return nil, err
	}

	cleanup := []string{}
	for _, d := range dirs {
		if _, ok := ids[d]; ok {
			continue
		}

		cleanup = append(cleanup, filepath.Join(snapshotDir, d))
	}

	return cleanup, nil
}

func getDiskQuotaSize(info *snapshots.Info) (string, error) {
	if info.Labels != nil {
		if size, ok := info.Labels[RootfsQuotaSnapshotterLabel]; ok {
			return size, nil
		}
	}
	return "", fmt.Errorf("no diskquota size parameter in labels")
}

func IsContainerType(info *snapshots.Info) bool {
	if info.Labels != nil {
		if op, ok := info.Labels[snapshots.TypeLabelKey]; ok {
			return op == snapshots.ContainerType
		}
	}
	return false
}

func (o *snapshotter) createSnapshot(ctx context.Context, kind snapshots.Kind, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	ctx, t, err := o.ms.TransactionContext(ctx, true)
	if err != nil {
		return nil, err
	}

	var td, path string
	defer func() {
		if err != nil {
			if td != "" {
				if err1 := os.RemoveAll(td); err1 != nil {
					log.G(ctx).WithError(err1).Warn("failed to cleanup temp snapshot directory")
				}
			}
			if path != "" {
				if err1 := os.RemoveAll(path); err1 != nil {
					log.G(ctx).WithError(err1).WithField("path", path).Error("failed to reclaim snapshot directory, directory may need removal")
					err = errors.Wrapf(err, "failed to remove path: %v", err1)
				}
			}
		}
	}()

	var tmpInfo snapshots.Info
	for _, opt := range opts {
		opt(&tmpInfo)
	}

	snapshotDir := filepath.Join(o.root, "snapshots")
	td, err = o.prepareDirectory(ctx, snapshotDir, kind)
	if err != nil {
		if rerr := t.Rollback(); rerr != nil {
			log.G(ctx).WithError(rerr).Warn("failed to rollback transaction")
		}
		return nil, errors.Wrap(err, "failed to create prepare snapshot dir")
	}
	rollback := true
	defer func() {
		if rollback {
			if rerr := t.Rollback(); rerr != nil {
				log.G(ctx).WithError(rerr).Warn("failed to rollback transaction")
			}
		}
	}()

	needToSetQuota := false
	var quotaID uint32
	// set diskquota when config enable diskquota and snapshot was created for container
	if IsContainerType(&tmpInfo) && kind == snapshots.KindActive {
		var existQuotaLabel bool
		if tmpInfo.Labels != nil && len(tmpInfo.Labels) != 0 {
			_, existQuotaLabel = tmpInfo.Labels[RootfsQuotaSnapshotterLabel]
		}
		if o.config.EnableDiskquota || existQuotaLabel {
			needToSetQuota = true
			//quotaID, err = o.quotaDriver.GetNextQuotaID()
			//if err != nil {
			//	return nil, errors.Wrapf(err, "failed to get next quota id")
			//}
			quotaID = diskquota.QuotaMinID
			// sync quota id to boltdb
			strID := strconv.FormatUint(uint64(quotaID), 10)
			opts = append(opts, snapshots.WithLabels(map[string]string{
				RootfsQuotaID: strID,
			}))
		}
	}

	s, err := storage.CreateSnapshot(ctx, kind, key, parent, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create snapshot")
	}

	if len(s.ParentIDs) > 0 {
		st, err := os.Stat(o.upperPath(s.ParentIDs[0]))
		if err != nil {
			return nil, errors.Wrap(err, "failed to stat parent")
		}

		stat := st.Sys().(*syscall.Stat_t)

		if err := os.Lchown(filepath.Join(td, "fs"), int(stat.Uid), int(stat.Gid)); err != nil {
			if rerr := t.Rollback(); rerr != nil {
				log.G(ctx).WithError(rerr).Warn("failed to rollback transaction")
			}
			return nil, errors.Wrap(err, "failed to chown")
		}
	}

	if needToSetQuota {
		diskQuotaSize, err := getDiskQuotaSize(&tmpInfo)
		if err != nil {
			log.G(ctx).WithError(err).Warnf("failed get diskquota size from lables, using default size: %s", o.config.DiskQuotaSize)
			diskQuotaSize = o.config.DiskQuotaSize
		}

		upperPath := filepath.Join(td, "fs")
		if err := o.setDiskQuota(ctx, upperPath, diskQuotaSize, quotaID); err != nil {
			return nil, errors.Wrapf(err, "failed to set diskquota on upperpath, snapshot id: %s", s.ID)
		}
		// if there's no parent, we just return a bind mount, so no need to set quota on workerpath
		if len(s.ParentIDs) > 0 {
			workpath := filepath.Join(td, "work")
			if err := o.setDiskQuota(ctx, workpath, diskQuotaSize, quotaID); err != nil {
				return nil, errors.Wrapf(err, "failed to set diskquota on workpath, snapshot id: %s", s.ID)
			}
		}
	}

	path = filepath.Join(snapshotDir, s.ID)
	if err = os.Rename(td, path); err != nil {
		return nil, errors.Wrap(err, "failed to rename")
	}
	td = ""

	rollback = false
	if err = t.Commit(); err != nil {
		return nil, errors.Wrap(err, "commit failed")
	}

	return o.mounts(s), nil
}

func (o *snapshotter) prepareDirectory(ctx context.Context, snapshotDir string, kind snapshots.Kind) (string, error) {
	td, err := ioutil.TempDir(filepath.Join(o.root, "snapshots"), "new-")
	if err != nil {
		return "", errors.Wrap(err, "failed to create temp dir")
	}

	if err := os.Mkdir(filepath.Join(td, "fs"), 0755); err != nil {
		return td, err
	}

	if kind == snapshots.KindActive {
		if err := os.Mkdir(filepath.Join(td, "work"), 0711); err != nil {
			return td, err
		}
	}

	return td, nil
}

func (o *snapshotter) mounts(s storage.Snapshot) []mount.Mount {
	if len(s.ParentIDs) == 0 {
		// if we only have one layer/no parents then just return a bind mount as overlay
		// will not work
		roFlag := "rw"
		if s.Kind == snapshots.KindView {
			roFlag = "ro"
		}

		return []mount.Mount{
			{
				Source: o.upperPath(s.ID),
				Type:   "bind",
				Options: []string{
					roFlag,
					"rbind",
				},
			},
		}
	}
	var options []string

	if s.Kind == snapshots.KindActive {
		options = append(options,
			fmt.Sprintf("workdir=%s", o.workPath(s.ID)),
			fmt.Sprintf("upperdir=%s", o.upperPath(s.ID)),
		)
	} else if len(s.ParentIDs) == 1 {
		return []mount.Mount{
			{
				Source: o.upperPath(s.ParentIDs[0]),
				Type:   "bind",
				Options: []string{
					"ro",
					"rbind",
				},
			},
		}
	}

	parentPaths := make([]string, len(s.ParentIDs))
	for i := range s.ParentIDs {
		parentPaths[i] = o.upperPath(s.ParentIDs[i])
	}

	options = append(options, fmt.Sprintf("lowerdir=%s", strings.Join(parentPaths, ":")))
	return []mount.Mount{
		{
			Type:    "overlay",
			Source:  "overlay",
			Options: options,
		},
	}

}

func (o *snapshotter) upperPath(id string) string {
	return filepath.Join(o.root, "snapshots", id, "fs")
}

func (o *snapshotter) workPath(id string) string {
	return filepath.Join(o.root, "snapshots", id, "work")
}

// Close closes the snapshotter
func (o *snapshotter) Close() error {
	return o.ms.Close()
}

// SetDiskQuota is used to set quota for directory.
func (o *snapshotter) setDiskQuota(ctx context.Context, dir string, size string, quotaID uint32) error {
	log.G(ctx).Info("setDiskQuota: dir %s, size %s", dir, size)
	if isRegular, err := diskquota.CheckRegularFile(dir); err != nil || !isRegular {
		log.G(ctx).Errorf("set quota skip not regular file: %s", dir)
		return err
	}

	id := o.quotaDriver.GetQuotaIDInFileAttr(dir)
	if id > 0 && id != quotaID {
		log.G(ctx).Errorf("quota id is already set, quota id: %s", id)
		return fmt.Errorf("quota id already set, quota is: %s", id)
	}

	log.G(ctx).Infof("try to set disk quota, dir(%s), size(%s), quotaID(%d)", dir, size, quotaID)

	if err := o.quotaDriver.SetDiskQuota(dir, size, quotaID); err != nil {
		return errors.Wrapf(err, "failed to set dir(%s) disk quota", dir)
	}
	return nil
}
