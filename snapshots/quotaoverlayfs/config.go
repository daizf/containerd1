package quotaoverlayfs

const RootfsQuotaID = "RootfsQuotaID"

// SnapshotterConfig is used to configure the overlay snapshotter instance
type SnapshotterConfig struct {
	// Enable diskQuota with overlayfs or not
	EnableDiskquota bool `toml:"enable_diskquota" json:"enable_diskquota"`

	// Defines the default size of diskquota
	DiskQuotaSize string `toml:"disk_quota_size" json:"disk_quota_size"`

	// Flag to async remove resource
	AsyncRemove bool `toml:"-"`
}

func defaultConfig() SnapshotterConfig {
	return SnapshotterConfig{
		EnableDiskquota: false,
		DiskQuotaSize:   "",
		AsyncRemove:     true,
	}
}
