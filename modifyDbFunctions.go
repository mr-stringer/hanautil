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
