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

const q_GetBackupCatalogEntryCount = "SELECT " +
	"COUNT(BACKUP_ID) " +
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

func f_GetTruncateDate(backupId string) string {
	return fmt.Sprintf("SELECT "+
		"COUNT(BACKUP_ID) AS FILES, "+
		"COALESCE(SUM(BACKUP_SIZE),0) AS BACKUP_SIZE "+
		"FROM "+
		"\"SYS\".\"M_BACKUP_CATALOG_FILES\" "+
		"WHERE "+
		"BACKUP_ID < '%s'", backupId)
}
