package hanautil

import "time"

// TraceFile is a struct that contains information about a HANA trace file
type TraceFile struct {
	Hostname      string
	FileName      string
	FileSizeBytes uint64
	LastModified  time.Time
}

// BackupSummary is a struct that contains information regarding HANA backups
type BackupSummary struct {
	BackupCatalogEntries      uint64
	FullBackups               uint64
	LogBackups                uint64
	IncrementalBackups        uint64
	DifferentialBackups       uint64
	LogMissing                uint64
	DataSnapshots             uint64
	SizeOfFullBackupsBytes    uint64
	SizeOfLogBackupBytes      uint64
	SizeOfIncrementalBackups  uint64
	SizeOfDifferentialBackups uint64
	SizeOfLogMissing          uint64
	SizeOfDataSnapshots       uint64
	SizeOfBackupCatalog       uint64
	OldestFullBackupDate      time.Time
	OldestLogBackupDate       time.Time
	CurrentDbTime             time.Time
}

// TruncateStats provided information regarding the number of files and the
// amount of data removed by truncating the backup catalog
type TruncateStats struct {
	FilesRemoved uint64
	BytesRemoved uint64
}

// LogSegmentsStats provides information about how much space is used by
// freeable and non-freeable log segments in the log volume
type LogSegmentsStats struct {
	FreeSegments             uint64
	TotalFreeSegmentBytes    uint64
	NonFreeSegments          uint64
	TotalNonFreeSegmentBytes uint64
}
