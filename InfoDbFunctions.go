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

// GetBackupSummary provides a summary of the number of backups along with
// aggregated backup size data found in the backup catalog. The dates of the
// oldest full and log backups in the catalog are also supplied.
func (h *HanaUtilClient) GetBackupSummary() (BackupSummary, error) {
	bs := BackupSummary{}

	r1 := h.db.QueryRow(q_GetBackupCatalogEntryCount)
	err := r1.Scan(&bs.BackupCatalogEntries)
	if err != nil {
		/*Promote the error*/
		return BackupSummary{}, err
	}

	r2, err := h.db.Query(q_GetBackupCount)
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

	r3, err := h.db.Query(q_GetBackupSizes)
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
