package hanautil

import (
	"testing"
	"time"
)

func TestBackupSummary_GetAllBytes(t *testing.T) {
	t1 := time.Now()
	type fields struct {
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
	tests := []struct {
		name   string
		fields fields
		want   uint64
	}{
		{"Good01", fields{100, 10, 90, 0, 0, 0, 0, 1024, 100, 0, 0, 0, 0, 0, t1, t1, t1}, 1124},
		{"Good02", fields{100, 10, 90, 0, 0, 0, 0, 1024, 100, 500, 500, 500, 500, 500, t1, t1, t1}, 3624},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bs := &BackupSummary{
				BackupCatalogEntries:      tt.fields.BackupCatalogEntries,
				FullBackups:               tt.fields.FullBackups,
				LogBackups:                tt.fields.LogBackups,
				IncrementalBackups:        tt.fields.IncrementalBackups,
				DifferentialBackups:       tt.fields.DifferentialBackups,
				LogMissing:                tt.fields.LogMissing,
				DataSnapshots:             tt.fields.DataSnapshots,
				SizeOfFullBackupsBytes:    tt.fields.SizeOfFullBackupsBytes,
				SizeOfLogBackupBytes:      tt.fields.SizeOfLogBackupBytes,
				SizeOfIncrementalBackups:  tt.fields.SizeOfIncrementalBackups,
				SizeOfDifferentialBackups: tt.fields.SizeOfDifferentialBackups,
				SizeOfLogMissing:          tt.fields.SizeOfLogMissing,
				SizeOfDataSnapshots:       tt.fields.SizeOfDataSnapshots,
				SizeOfBackupCatalog:       tt.fields.SizeOfBackupCatalog,
				OldestFullBackupDate:      tt.fields.OldestFullBackupDate,
				OldestLogBackupDate:       tt.fields.OldestLogBackupDate,
				CurrentDbTime:             tt.fields.CurrentDbTime,
			}
			if got := bs.GetAllBytes(); got != tt.want {
				t.Errorf("BackupSummary.GetAllBytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataVolumeFragStats_CalculateFragPct(t *testing.T) {
	type fields struct {
		Host                string
		Port                uint64
		Service             string
		DataVolumeBytes     uint64
		DataVolumeUsedBytes uint64
		FragPct             float32
	}
	tests := []struct {
		name   string
		fields fields
		want   float32
	}{
		{"Good01", fields{"hdb1", 30013, "nameserver", 10000000, 8000000, 0}, 20.0},
		{"Good02", fields{"hdb1", 30013, "nameserver", 2048000000, 921600000, 0}, 55.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DataVolumeFragStats{
				Host:                tt.fields.Host,
				Port:                tt.fields.Port,
				Service:             tt.fields.Service,
				DataVolumeBytes:     tt.fields.DataVolumeBytes,
				DataVolumeUsedBytes: tt.fields.DataVolumeUsedBytes,
				FragPct:             tt.fields.FragPct,
			}
			d.CalculateFragPct()
			if tt.want != d.FragPct {
				t.Errorf("TestDataVolumeFragStats.CalculateFragPct.FragPct = %f, want %f", d.FragPct, tt.want)
			}
		})
	}
}
