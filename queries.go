package hanautil

import "fmt"

/******************************************************************************/
/* The file contains all the queries used in the library.                     */
/* Queries are all written exclusively in UPPERCASE.                          */
/* Schemas and tables are enclosed in double quotes                           */
/* Static queries have the q_ naming convention                               */
/* Functions that return a query string have the f_ naming convention         */
/******************************************************************************/

const q_GetHanaVersion = "SELECT VERSION FROM \"SYS\".\"M_DATABASE\""

const q_GetDbCurrentTime = "SELECT NOW() AS \"CURRENT_TIME\" FROM DUMMY"

const q_GetBackupCatalogEntryCount = "SELECT " +
	"COUNT(BACKUP_ID) AS COUNT " +
	"FROM \"SYS\".\"M_BACKUP_CATALOG\""

const q_GetBackupCount = "SELECT " +
	"COUNT(ENTRY_ID) AS COUNT, " +
	"ENTRY_TYPE_NAME " +
	"FROM \"SYS\".\"M_BACKUP_CATALOG\" " +
	"GROUP BY ENTRY_TYPE_NAME"

const q_GetBackupSizes = "SELECT " +
	"CAT.ENTRY_TYPE_NAME AS TYPE,  " +
	"SUM(FILES.BACKUP_SIZE) AS BYTES  " +
	"FROM \"SYS\".\"M_BACKUP_CATALOG\" AS CAT  " +
	"LEFT JOIN \"SYS\".\"M_BACKUP_CATALOG_FILES\" AS FILES " +
	"ON CAT.BACKUP_ID = FILES.BACKUP_ID  " +
	"GROUP BY CAT.ENTRY_TYPE_NAME"

const q_GetOldestBackups = "SELECT " +
	"ENTRY_TYPE_NAME, " +
	"MIN(UTC_START_TIME) AS UTC_START_NAME " +
	"FROM \"SYS\".\"M_BACKUP_CATALOG\"" +
	"WHERE " +
	"ENTRY_TYPE_NAME = 'complete data backup' OR ENTRY_TYPE_NAME = 'log backup' " +
	"GROUP BY ENTRY_TYPE_NAME"

const q_GetBackupCatalogSize = "SELECT TOP 1 " +
	"BF.BACKUP_SIZE " +
	"FROM " +
	"\"SYS\".\"M_BACKUP_CATALOG\" B, " +
	"\"SYS\".\"M_BACKUP_CATALOG_FILES\" BF " +
	"WHERE " +
	"B.BACKUP_ID = BF.BACKUP_ID AND " +
	"BF.SOURCE_TYPE_NAME = 'catalog' AND " +
	"B.STATE_NAME = 'successful' " +
	"ORDER BY " +
	"B.SYS_START_TIME DESC; "

const q_GetLogSegmentStats = "SELECT " +
	"STATE, " +
	"COUNT(STATE) AS SEGMENTS, " +
	"COALESCE(SUM(TOTAL_SIZE),0) AS BYTES " +
	"FROM  " +
	"\"SYS\".\"M_LOG_SEGMENTS\" " +
	"WHERE " +
	"STATE = 'Free' GROUP BY STATE " +
	"UNION ALL " +
	"SELECT " +
	"'NonFree' AS STATE, " +
	"COUNT(STATE) AS SEGMENTS, " +
	"COALESCE(SUM(TOTAL_SIZE),0) AS BYTES " +
	"FROM  " +
	"\"SYS\".\"M_LOG_SEGMENTS\" " +
	"WHERE STATE != 'Free';"

const q_GetFreeLogBytes string = "SELECT " +
	"COALESCE(SUM(TOTAL_SIZE),0) AS BYTES " +
	"FROM \"SYS\".\"M_LOG_SEGMENTS\" " +
	"WHERE STATE = 'Free';"

const q_ReclaimLog string = "ALTER SYSTEM RECLAIM LOG"

const q_GetDataDefrag string = "SELECT " +
	"\"SYS\".\"HOST\", " +
	"\"SYS\".\"PORT\", " +
	"\"SRV\".\"SERVICE_NAME\", " +
	"\"SYS\".\"TOTAL_SIZE\", " +
	"\"SYS\".\"USED_SIZE\" " +
	"FROM " +
	"\"SYS\".\"M_VOLUME_FILES\" AS SYS " +
	"LEFT JOIN \"SYS\".\"M_SERVICES\" AS SRV " +
	"ON \"SYS\".PORT = \"SRV\".\"PORT\" " +
	"WHERE \"SYS\".\"FILE_TYPE\" = 'DATA';"

// q_DefragDataVollAll returns the query that will be used to shrink all data
// volumes on all hosts. It returns an they query and an error. The function
// take the argument pct, which represents the pct size of the data volume after
// de-fragmentation. The Pct value may be no lower than 105. Values between 105
// and 120 are recommended. Although not technically invalid, the function will
// return an error if pct is set above 150. If the value of pct is 0, the
// default value of 120 will be applied.
func q_DefragDataVolAll(pct uint) (string, error) {
	/*Ensure that pct is OK*/
	switch {
	case pct == 0:
		pct = 120
	case pct < 105:
		return "", fmt.Errorf("pct too low, must be between 105 and 105")
	case pct > 150:
		return "", fmt.Errorf("pct too low, must be between 105 and 105")
	}
	return fmt.Sprintf("ALTER SYSTEM RECLAIM DATAVOLUME %d DEFRAGMENT", pct), nil
}

// q_DefragDataVoll returns the query that will be used to shrink a specifc data
// volume on a specific host. It returns an they query and an error.
// The function takes three arguments host, port and pct. host is used to
// identify the specific host where the volume to defrag resides. port specifies
// the port of the data volume to defrag. pct represents the pct size of the
// data volume after de-fragmentation. The Pct value may be no lower than 105.
// Values between 105 and 120 are recommended. Although not technically invalid,
// the function will return an error if pct is set above 150. If the value of
// pct is 0, the default value of 120 will be applied.
func q_DefragDataVol(host string, port, pct uint) (string, error) {
	switch {
	case host == "":
		return "", fmt.Errorf("input variable host cannot be empty")
	case port == 0:
		return "", fmt.Errorf("input variable port cannot be 0")
	case pct == 0:
		pct = 120
	case pct < 105:
		return "", fmt.Errorf("pct too low, must be between 105 and 105")
	case pct > 150:
		return "", fmt.Errorf("pct too low, must be between 105 and 105")
	}
	return fmt.Sprintf("ALTER SYSTEM RECLAIM DATAVOLUME '%s:%d' %d DEFRAGMENT", host, port, pct), nil
}

func q_GetDataVolume(host string, port uint) string {
	return fmt.Sprintf("SELECT "+
		"\"SYS\".\"HOST\", "+
		"\"SYS\".\"PORT\", "+
		"\"SRV\".\"SERVICE_NAME\", "+
		"\"SYS\".\"TOTAL_SIZE\", "+
		"\"SYS\".\"USED_SIZE\" "+
		"FROM "+
		"\"SYS\".\"M_VOLUME_FILES\" AS SYS "+
		"LEFT JOIN \"SYS\".\"M_SERVICES\" AS SRV "+
		"ON \"SYS\".\"PORT\" = \"SRV\".\"PORT\" "+
		"WHERE \"SYS\".\"FILE_TYPE\" = 'DATA' "+
		"AND "+
		"\"SYS\".\"HOST\" = '%s' "+
		"AND "+
		"\"SYS\".\"PORT\" = '%d';", host, port)
}

func q_GetLatestFullBackupID(days uint) string {
	return fmt.Sprintf("SELECT "+
		"BACKUP_ID "+
		"FROM \"SYS\".\"M_BACKUP_CATALOG\""+
		"WHERE STATE_NAME = 'successful' "+
		"AND "+
		"ENTRY_TYPE_NAME = 'complete data backup' "+
		"AND SYS_END_TIME < ("+
		"SELECT ADD_DAYS(NOW(),-%d) FROM DUMMY) "+
		"ORDER BY SYS_END_TIME DESC LIMIT 1", days)
}

func f_GetTraceFile(host, filename string) string {
	return fmt.Sprintf("SELECT COUNT(FILE_NAME) AS COUNT FROM \"SYS\".\"M_TRACEFILES\" WHERE HOST = '%s' AND FILE_NAME = '%s'", host, filename)
}

func f_GetTraceFiles(days uint) string {
	return fmt.Sprintf("SELECT HOST, FILE_NAME, FILE_SIZE, FILE_MTIME FROM \"SYS\".\"M_TRACEFILES\" WHERE FILE_MTIME < (SELECT ADD_DAYS(NOW(), -%d) FROM DUMMY) AND RIGHT(FILE_NAME, 3) = 'trc' OR FILE_MTIME < (SELECT ADD_DAYS(NOW(), -%d) FROM DUMMY) AND RIGHT(FILE_NAME, 2) = 'gz'", days, days)
}

// Returns a string query that is used to attempt to remove the identified trace
// file
// Require TRACE ADMIN priv
func f_RemoveTraceFile(hostname, filename string) string {
	return fmt.Sprintf("ALTER SYSTEM REMOVE TRACES('%s', '%s')", hostname, filename)
}

// Returns a string that is used to remove old backup catalog entries. This
// statement will not destroy backup media. The statement will remove all
// entries older than the backup ID given. The given backup ID must be a full
// backup
func f_GetBackupDelete(backupId string) string {
	return fmt.Sprintf("BACKUP CATALOG DELETE ALL BEFORE BACKUP_ID %s", backupId)
}

// Same as above but will also destroy the files from the file system or
// backint
func f_GetBackupDeleteComplete(backupId string) string {
	return fmt.Sprintf("BACKUP CATALOG DELETE ALL BEFORE BACKUP_ID %s COMPLETE", backupId)
}

// Truncate the backup catalog
func f_GetTruncateData(backupId string) string {
	return fmt.Sprintf("SELECT "+
		"COUNT(BACKUP_ID) AS FILES, "+
		"COALESCE(SUM(BACKUP_SIZE),0) AS BACKUP_SIZE "+
		"FROM "+
		"\"SYS\".\"M_BACKUP_CATALOG_FILES\" "+
		"WHERE "+
		"BACKUP_ID < '%s'", backupId)
}

// Get the number of stat alert server alerts older then given 'days' parameter
func f_GetStatServerAlerts(days uint) string {
	return fmt.Sprintf("SELECT COUNT(SNAPSHOT_ID) AS COUNT FROM \"_SYS_STATISTICS\".\"STATISTICS_ALERTS_BASE\" WHERE ALERT_TIMESTAMP < ADD_DAYS(NOW(), -%d)", days)
}

// statement to remove alerts older than the given number of days
func f_RemoveStatServerAlerts(days uint) string {
	return fmt.Sprintf("DELETE FROM "+
		"\"_SYS_STATISTICS\".\"STATISTICS_ALERTS_BASE\" "+
		"WHERE ALERT_TIMESTAMP < ADD_DAYS(NOW(), -%d)", days)
}

func f_GetBackupCatalogEntryCountBeforeID(s string) string {
	return fmt.Sprintf("SELECT "+
		"COUNT(BACKUP_ID) AS COUNT "+
		"FROM \"SYS\".\"M_BACKUP_CATALOG\""+
		"WHERE BACKUP_ID < '%s'", s)
}

func f_GetBackupCountBeforeID(s string) string {
	return fmt.Sprintf("SELECT "+
		"COUNT(ENTRY_ID) AS COUNT, "+
		"ENTRY_TYPE_NAME "+
		"FROM \"SYS\".\"M_BACKUP_CATALOG\" "+
		"WHERE BACKUP_ID < '%s' "+
		"GROUP BY ENTRY_TYPE_NAME", s)
}

func f_GetBackupSizesBeforeId(s string) string {
	return fmt.Sprintf("SELECT "+
		"CAT.ENTRY_TYPE_NAME AS TYPE, "+
		"SUM(FILES.BACKUP_SIZE) AS BYTES "+
		"FROM \"SYS\".\"M_BACKUP_CATALOG\" AS CAT "+
		"LEFT JOIN \"SYS\".\"M_BACKUP_CATALOG_FILES\" AS FILES "+
		"ON CAT.BACKUP_ID = FILES.BACKUP_ID "+
		"WHERE CAT.BACKUP_ID < '%s' "+
		"GROUP BY CAT.ENTRY_TYPE_NAME", s)
}
