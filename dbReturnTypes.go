package hanautil

import "time"

type TraceFile struct {
	Hostname      string
	FileName      string
	FileSizeBytes uint64
	LastModified  time.Time
}

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
}

type TruncateStats struct {
	FilesRemoved uint64
	BytesRemoved uint64
}

type LogSegmentsStats struct {
	FreeSegments             uint64
	TotalFreeSegmentBytes    uint64
	NonFreeSegments          uint64
	TotalNonFreeSegmentBytes uint64
}
