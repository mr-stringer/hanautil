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
