package hanautil

import (
	"fmt"
	"time"
)

/******************************************************************************/
/* This file contains functions that get information from the HANA databases  */
/******************************************************************************/

// GetVersion returns the version of the the HANA database and an error.
func (h *HanaUtilClient) GetVersion() (string, error) {
	r1 := h.db.QueryRow(q_GetHanaVersion)
	var version string
	err := r1.Scan(&version)
	if err != nil {
		/*elevate error*/
		return version, err
	}
	return version, nil
}

// GetTraceFiles retrieves information about HANA database traces.
// It returns a slice of the type 'TraceFiles' and an error.  The
// argument 'days' is used to filter the returned results to trace files that
// that have a modification date the exceeds the argument 'days'.  For example,
// if days were set to '1', only the returned slice would only include details
// of trace files where the modification date is was longer that 24 hours ago.
func (h *HanaUtilClient) GetTraceFiles(days uint) ([]TraceFile, error) {
	TraceFiles := make([]TraceFile, 0)
	rows, err := h.db.Query(f_GetTraceFiles(days))

	if err != nil {
		/*Promote error*/
		return nil, err
	}
	defer rows.Close()

	/*Wrangle rows into type*/
	for rows.Next() {
		row := TraceFile{}
		err = rows.Scan(&row.Hostname, &row.FileName, &row.FileSizeBytes, &row.LastModified)
		if err != nil {
			/*Promote error*/
			return nil, err
		}
		TraceFiles = append(TraceFiles, row)
	}
	return TraceFiles, nil
}

// GetBackupSummary provides a summary of the number of backups along
// aggregated backup size data found in the backup catalog. The dates of the
// oldest full and log backups in the catalog are also supplied.
func (h *HanaUtilClient) GetBackupSummary() (BackupSummary, error) {
	bs, err := h.fetchBackupStats("")
	if err != nil {
		return bs, err
	}
	return bs, nil
}

// GetFullBackupID returns the latest full backup that is older than the given
// days in the days argument. The output of this may then be used by
// GetBackupSummaryBeforeBackupID for information about data that could be
// removed if a truncation is applied.
func (h *HanaUtilClient) GetFullBackupId(days int) (string, error) {
	var s string

	r1 := h.db.QueryRow(q_GetLatestFullBackupID(uint(days)))
	err := r1.Scan(&s)
	if err != nil {
		//elevate error
		return s, err
	}

	return s, nil
}

// GetBackupSummaryBeforeBackupID provides a summary of the number of backups
// aggregated backup size data found in the backup catalog that occur before a
// given backup ID. The dates of the oldest full and log backups in the catalog
// are also supplied.
func (h *HanaUtilClient) GetBackupSummaryBeforeBackupID(b string) (BackupSummary, error) {
	bs, err := h.fetchBackupStats(b)
	if err != nil {
		return bs, err
	}
	return bs, nil
}

func (h *HanaUtilClient) fetchBackupStats(backupID string) (BackupSummary, error) {
	bs := BackupSummary{}
	var q1, q2, q3 string
	if backupID == "" {
		q1 = q_GetBackupCatalogEntryCount
		q2 = q_GetBackupCount
		q3 = q_GetBackupSizes
	} else {
		q1 = f_GetBackupCatalogEntryCountBeforeID(backupID)
		q2 = f_GetBackupCountBeforeID(backupID)
		q3 = f_GetBackupSizesBeforeId(backupID)
	}

	r1 := h.db.QueryRow(q1)
	err := r1.Scan(&bs.BackupCatalogEntries)
	if err != nil {
		/*Promote the error*/
		return BackupSummary{}, err
	}

	r2, err := h.db.Query(q2)
	if err != nil {
		/*Promote database error*/
		return BackupSummary{}, err
	}
	defer r2.Close()

	for r2.Next() {
		var tmpCount uint64
		var tmpType string
		err = r2.Scan(&tmpCount, &tmpType)
		if err != nil {
			/*PromoteError*/
			return BackupSummary{}, err
		}

		switch tmpType {
		case "complete data backup":
			bs.FullBackups = tmpCount
		case "incremental data backup":
			bs.IncrementalBackups = tmpCount
		case "differential data backup":
			bs.DifferentialBackups = tmpCount
		case "log backup":
			bs.LogBackups = tmpCount
		case "log missing":
			bs.LogMissing = tmpCount
		case "data snapshot":
			bs.DataSnapshots = tmpCount
		default:
			return BackupSummary{}, fmt.Errorf("UnexpectedBackupType")
		}
	}

	r3, err := h.db.Query(q3)
	if err != nil {
		/*Promote database error*/
		return BackupSummary{}, err
	}
	defer r3.Close()

	for r3.Next() {
		var tmpType string
		var tmpCount uint64
		err = r3.Scan(&tmpType, &tmpCount)
		if err != nil {
			/*PromoteError*/
			return BackupSummary{}, err
		}
		switch tmpType {
		case "complete data backup":
			bs.SizeOfFullBackupsBytes = tmpCount
		case "incremental data backup":
			bs.SizeOfIncrementalBackups = tmpCount
		case "differential data backup":
			bs.SizeOfDifferentialBackups = tmpCount
		case "log backup":
			bs.SizeOfLogBackupBytes = tmpCount
		case "log missing":
			bs.SizeOfLogMissing = tmpCount
		case "data snapshot":
			bs.SizeOfDataSnapshots = tmpCount
		default:
			return BackupSummary{}, fmt.Errorf("UnexpectedBackupType")
		}
	}

	r4, err := h.db.Query(q_GetOldestBackups)
	if err != nil {
		/*Promote database error*/
		return BackupSummary{}, err
	}
	defer r4.Close()

	for r4.Next() {
		var tmpType string
		var tmpDate time.Time
		err = r4.Scan(&tmpType, &tmpDate)
		if err != nil {
			/*Promote error*/
			return BackupSummary{}, err
		}
		switch tmpType {
		case "complete data backup":
			bs.OldestFullBackupDate = tmpDate
		case "log backup":
			bs.OldestLogBackupDate = tmpDate
		default:
			return BackupSummary{}, fmt.Errorf("UnexpectedBackupType")
		}
	}

	var backupCatalogSize uint64
	r5 := h.db.QueryRow(q_GetBackupCatalogSize)
	err = r5.Scan(&backupCatalogSize)
	if err != nil {
		/*PromoteError*/
		return BackupSummary{}, err
	}
	bs.SizeOfBackupCatalog = backupCatalogSize

	r6 := h.db.QueryRow(q_GetDbCurrentTime)
	err = r6.Scan(&bs.CurrentDbTime)
	if err != nil {
		/*Promote the error*/
		return BackupSummary{}, err
	}

	return bs, nil
}

// GetStatServerAlerts is a function that reports the number of historic alerts
// that are stored in the _SYS_STATISTICS.STATISTICS_ALERTS_BASE table. SAP HANA
// minichecks will flag any database where there are alerts in the tables that
// are older than 42 days. This function will return the number of alerts that
// are more than the 'days' argument old.
// Errors returned are either 'UnexpectedDbReturn', when the query produces an
// unexpected value or a DB driver error promoted directly from the DB.
func (h *HanaUtilClient) GetStatServerAlerts(days uint) (uint, error) {
	var alerts uint
	r1 := h.db.QueryRow(f_GetStatServerAlerts(days))
	err := r1.Scan(&alerts)
	if err != nil {
		/*PromoteError*/
		return alerts, err
	}
	return alerts, err
}

// GetLogSegmentStats provides information about the free and non-free
// log segments in the HANA log volume
func (h *HanaUtilClient) GetLogSegmentStats() (LogSegmentsStats, error) {
	ls := LogSegmentsStats{}
	r1, err := h.db.Query(q_GetLogSegmentStats)
	if err != nil {
		/*PromoteError*/
		return LogSegmentsStats{}, err
	}

	for r1.Next() {
		var tmpState string
		var tmpBytes, tmpSegments uint64
		err := r1.Scan(&tmpState, &tmpSegments, &tmpBytes)
		if err != nil {
			/*PromoteError*/
			return LogSegmentsStats{}, err
		}
		switch tmpState {
		case "Free":
			ls.FreeSegments = tmpSegments
			ls.TotalFreeSegmentBytes = tmpBytes
		case "NonFree":
			ls.NonFreeSegments = tmpSegments
			ls.TotalNonFreeSegmentBytes = tmpBytes
		default:
			return LogSegmentsStats{}, fmt.Errorf("UnexpectedDbReturn")
		}

	}

	return ls, nil
}

// GetFragStats returns a slice of the type DataVolumeFragStats. The stats
// contain fragmentation stats for all data volumes on the system
func (h *HanaUtilClient) GetDataFragStats() ([]DataVolumeFragStats, error) {
	ret := []DataVolumeFragStats{}
	rows, err := h.db.Query(q_GetDataDefrag)
	if err != nil {
		/*Promote Error*/
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		df := DataVolumeFragStats{}
		err = rows.Scan(&df.Host, &df.Port, &df.Service, &df.DataVolumeBytes, &df.DataVolumeUsedBytes)
		if err != nil {
			/*Promote Error*/
			return nil, err
		}
		df.CalculateFragPct()
		ret = append(ret, df)
	}
	return ret, nil
}

// GetVolFragStats returns a pointer to aa single DataVolmeFragStats of a
// specific data volume on a specific host. The function also returns an error.
// If an error is found the returned DataVolumeFragStats will be nil pointer.
// The function takes two arguments, 'host' and 'port'. If the user is not sure
// of these the function 'GetDataFragStats' can be used to provide these.
// If no datavolume matching the host and port is not found, the error will be
// 'dataVolumeNotFound'
func (h *HanaUtilClient) GetVolFragStats(host string, port uint) (*DataVolumeFragStats, error) {
	ret := DataVolumeFragStats{}
	/* Our query should produce one row, but we run Query and check the */
	/* returned rows anyway */
	rows, err := h.db.Query(q_GetDataVolume(host, port))
	if err != nil {
		//promote error
		return nil, err
	}
	defer rows.Close()

	c1 := false
	for rows.Next() {
		if c1 == true {
			return nil, fmt.Errorf("more than one row discovered, this is unexpected")
		}
		err = rows.Scan(&ret.Host, &ret.Port, &ret.Service, &ret.DataVolumeBytes, &ret.DataVolumeUsedBytes)
		if err != nil {
			/*Promote Error*/
			return nil, err
		}
		ret.CalculateFragPct()
		c1 = true
	}
	// Check that a row was found
	if c1 == false {
		return nil, fmt.Errorf("dataVolumeNotFound")
	}
	return &ret, nil
}
