package hanautil

import "fmt"

// The RemoveTraceFile function deletes HANA trace files. Use the the
// GetTraceFiles function to find candidates for removal. The function takes two
// arguments, the HANA host that the trace files resides upon and the trace file
// name.
//
// hanautil will first check to ascertain if the file requested for delete
// exists. If it does not, the error 'TraceFileNotFound' will be returned. If
// the requested file is currently open, it will not be removed, in such a case
// the error 'TraceFileNotRemoved' will be returned. Any database errors
// discovered will be promoted as the returned error of this function. If the
// returned error is 'nil', then the file was successfully removed.
//
// In the unlikely occurrence that host and file name combination does not yield
// a unique result, the error 'TraceFileNotUnique' will be returned.
func (h *hanaUtilClient) RemoveTraceFile(host, filename string) error {
	r1 := h.db.QueryRow(f_GetTraceFile(host, filename))
	var count uint32
	err := r1.Scan(&count)
	if err != nil {
		return err
	}

	if count < 1 {
		return fmt.Errorf("TraceFileNotFound")
	} else if count > 1 {
		return fmt.Errorf("TraceFileNotUnique")
	}

	_, err = h.db.Exec(f_RemoveTraceFile(host, filename))
	if err != nil {
		// Promote DB error
		return err
	}

	/*As we can't check if a trace file is actually open or not, check if it
	still exists and if it does return the 'TraceFileNotRemoved' error*/
	r3 := h.db.QueryRow(f_GetTraceFile(host, filename))
	err = r3.Scan(&count)
	if err != nil {
		return err
	}

	if count != 0 {
		return fmt.Errorf("TraceFileNotRemoved")
	}

	return nil
}

// TruncateBackupCatalog removes entries from the HANA database backup catalog
// with the option of permanently destroying associated physical files.
// A large HANA backup catalog can cause performance issues and is recommeded to
// be <50MiB
func (h *hanaUtilClient) TruncateBackupCatalog(days int, complete bool) (TruncateStats, error) {
	tr := TruncateStats{}
	//First find the last full backup that is older than the given days
	r1 := h.db.QueryRow(q_GetLatestFullBackupID(uint(days)))
	var backupId string
	err := r1.Scan(&backupId)
	if err != nil {
		/*PromoteError*/
		return tr, err
	}

	var truncFiles uint64
	var truncBytes uint64
	r2 := h.db.QueryRow(f_GetTruncateData(backupId))
	err = r2.Scan(&truncFiles, &truncBytes)
	if err != nil {
		/*PromoteError*/
		return tr, err
	}

	if complete {
		_, err = h.db.Exec(f_GetBackupDeleteComplete(backupId))
		if err != nil {
			/*Promote error*/
			return tr, err
		}
	} else {
		_, err = h.db.Exec(f_GetBackupDelete(backupId))
		if err != nil {
			/*Promote error*/
			return tr, err
		}
	}

	/*Hopefully, all of the truncated stuff should be gone, but we need to
	check */
	var postTruncFiles uint64
	var postTruncBytes uint64
	r3 := h.db.QueryRow(f_GetTruncateData(backupId))
	err = r3.Scan(&postTruncFiles, &postTruncBytes)
	if err != nil {
		/*PromoteError*/
		return TruncateStats{}, err
	}

	/*Always report number of removed files / entries */
	/*Mitigation around potentially going less than zero on uint vars*/
	if postTruncFiles < truncFiles {
		tr.FilesRemoved = truncFiles - postTruncFiles
	} else {
		tr.FilesRemoved = truncFiles
	}

	/*Report bytes only in complete mode*/
	if complete {
		/*Mitigation around potentially going less than zero on uint vars*/
		if postTruncBytes < truncBytes {
			tr.BytesRemoved = truncBytes - postTruncBytes
		} else {
			tr.BytesRemoved = truncBytes
		}
	} else {
		tr.BytesRemoved = 0
	}

	return tr, nil
}

// RemoveStatServerAlerts removes entries from the
// SYS_STATISTICS.STATISTICS_ALERTS_BASE table that are older than the number of
// days given in the 'days' argument.
// The function returns a uint64 which
func (h *hanaUtilClient) RemoveStatServerAlerts(days uint) (uint64, error) {
	var preRemove uint64
	r1 := h.db.QueryRow(f_GetStatServerAlerts(days))
	err := r1.Scan(&preRemove)
	if err != nil {
		/*PromoteError*/
		return 0, err
	}

	/*Now do the deletion*/
	_, err = h.db.Exec(f_RemoveStatServerAlerts(days))
	if err != nil {
		/*PromoteError*/
		return 0, err
	}
	var postRemove uint64
	r2 := h.db.QueryRow(f_GetStatServerAlerts(days))
	err = r2.Scan(&postRemove)
	if err != nil {
		/*PromoteError*/
		return 0, err
	}

	/*Although its unlikely, there is a chance that the not only do no alerts
	get removed but quailify for the second query, which would lead to a
	negative uint (which can't happen), so if the second query has a larger
	total we assume that no alerts we removed and return a 0*/
	if postRemove <= preRemove {
		return preRemove - postRemove, nil
	} else {
		return 0, nil
	}
}
