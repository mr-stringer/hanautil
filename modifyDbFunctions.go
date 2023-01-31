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

	return err
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
	r2 := h.db.QueryRow(f_GetTruncateDate(backupId))
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

	/*Hopefull, all of the truncated stuff should be gone, but we need to
	check */
	var postTruncFiles uint64
	var postTruncBytes uint64
	r3 := h.db.QueryRow(f_GetTruncateDate(backupId))
	err = r3.Scan(&postTruncFiles, &postTruncBytes)
	if err != nil {
		/*PromoteError*/
		return tr, err
	}

	/*There's a risk here of going minus on the uints
	fixed later */
	tr.FilesRemoved = truncFiles - postTruncFiles
	tr.BytesRemoved = truncBytes - postTruncBytes

	return tr, nil
}
