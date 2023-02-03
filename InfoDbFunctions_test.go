package hanautil

import (
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func Test_hanaUtilClient_GetVersion(t *testing.T) {
	/*Test Setup*/
	/*Mock DB*/
	db1, mock, err := sqlmock.New()
	if err != nil {
		t.Errorf("an error '%s' was not expected when opening mock database connection", err)
	}
	defer db1.Close()

	type fields struct {
		db  *sql.DB
		dsn string
	}
	tests := []struct {
		name    string
		fields  fields
		want    string
		wantErr bool
	}{
		{"Good", fields{db1, ""}, "2.00.00.00", false},
		{"DbError", fields{db1, ""}, "", true},
	}
	for _, tt := range tests {
		/*per case mocking*/
		switch {
		case tt.name == "Good":
			row := sqlmock.NewRows([]string{"VERSION"}).AddRow("2.00.00.00")
			mock.ExpectQuery(q_GetHanaVersion).WillReturnRows(row)
		case tt.name == "DbError":
			mock.ExpectQuery(q_GetHanaVersion).WillReturnError(fmt.Errorf("DB ERROR"))
		default:
			fmt.Println("No test case matched")
			t.Errorf("No test case matched")

		}
		t.Run(tt.name, func(t *testing.T) {
			h := &hanaUtilClient{
				db:  tt.fields.db,
				dsn: tt.fields.dsn,
			}
			got, err := h.GetVersion()
			if (err != nil) != tt.wantErr {
				t.Errorf("hanaUtilClient.GetVersion() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("hanaUtilClient.GetVersion() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_hanaUtilClient_GetTraceFiles(t *testing.T) {
	/*Test Setup*/
	/*Mock DB*/
	db1, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Errorf("an error '%s' was not expected when opening mock database connection", err)
	}
	defer db1.Close()

	//Generic timestamp
	genTime := time.Date(2022, 1, 1, 12, 0, 0, 0, time.UTC)

	type fields struct {
		db  *sql.DB
		dsn string
	}
	type args struct {
		days uint
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []TraceFile
		wantErr bool
	}{
		{"SingleResult", fields{db1, ""}, args{7},
			[]TraceFile{{"hana01", "nameserver_hana01.30001.000.trc", 64000, genTime}}, false},
		{"MultipleResults", fields{db1, ""}, args{7},
			[]TraceFile{
				{"hana01", "nameserver_hana01.30001.000.trc", 64000, genTime},
				{"hana01", "indexserver_hana01.30001.000.trc", 128000, genTime}}, false},
		{"DbError", fields{db1, ""}, args{7}, nil, true},
		{"ScanError", fields{db1, ""}, args{7}, nil, true},
	}
	for _, tt := range tests {
		/*per case mocking*/
		switch {
		case tt.name == "SingleResult":
			row := sqlmock.NewRows([]string{"HOST", "FILE_NAME", "FILE_SIZE", "FILE_MTIME"})
			row.AddRow("hana01", "nameserver_hana01.30001.000.trc", 64000, genTime)
			mock.ExpectQuery(f_GetTraceFiles(7)).WillReturnRows(row)
		case tt.name == "MultipleResults":
			rows := sqlmock.NewRows([]string{"HOST", "FILE_NAME", "FILE_SIZE", "FILE_MTIME"})
			rows.AddRow("hana01", "nameserver_hana01.30001.000.trc", 64000, genTime)
			rows.AddRow("hana01", "indexserver_hana01.30001.000.trc", 128000, genTime)
			mock.ExpectQuery(f_GetTraceFiles(7)).WillReturnRows(rows)
		case tt.name == "DbError":
			mock.ExpectQuery(f_GetTraceFiles(7)).WillReturnError(fmt.Errorf("DbError"))
		case tt.name == "ScanError":
			row := sqlmock.NewRows([]string{"HOST", "FILE_NAME", "FILE_SIZE", "FILE_MTIME"})
			row.AddRow("hana01", "nameserver_hana01.30001.000.trc", 64000.1, genTime)
			mock.ExpectQuery(f_GetTraceFiles(7)).WillReturnRows(row)
		default:
			fmt.Println("No test case matched")
			t.Errorf("No test case matched")

		}
		t.Run(tt.name, func(t *testing.T) {
			h := &hanaUtilClient{
				db:  tt.fields.db,
				dsn: tt.fields.dsn,
			}
			got, err := h.GetTraceFiles(tt.args.days)
			if (err != nil) != tt.wantErr {
				t.Errorf("hanaUtilClient.GetTraceFiles() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("hanaUtilClient.GetTraceFiles() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_hanaUtilClient_GetBackupSummary(t *testing.T) {
	/*Test Setup*/
	/*Mock DB*/
	db1, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Errorf("an error '%s' was not expected when opening mock database connection", err)
	}
	defer db1.Close()

	//Generic timestamp
	genTime := time.Date(2022, 1, 1, 12, 0, 0, 0, time.UTC)

	type fields struct {
		db  *sql.DB
		dsn string
	}
	tests := []struct {
		name    string
		fields  fields
		want    BackupSummary
		wantErr bool
	}{
		{"Good01", fields{db1, ""}, BackupSummary{
			100, 10, 90, 0, 0, 0, 0, 1024000, 512000, 0, 0, 0, 0, 10240, genTime, genTime}, false},
		{"Good02", fields{db1, ""}, BackupSummary{
			60, 10, 10, 10, 10, 10, 10, 1024, 1024, 1024, 1024, 1024, 1024, 10240, genTime, genTime}, false},
		{"BackupCatalogSizeDbError", fields{db1, ""}, BackupSummary{}, true},
		{"OldestBackupUnexpectedResult", fields{db1, ""}, BackupSummary{}, true},
		{"OldestBackupScanError", fields{db1, ""}, BackupSummary{}, true},
		{"OldestBackupDbError", fields{db1, ""}, BackupSummary{}, true},
		{"BackupSizeUnexpectedResult", fields{db1, ""}, BackupSummary{}, true},
		{"BackupSizeDbError", fields{db1, ""}, BackupSummary{}, true},
		{"BackupSizeScanError", fields{db1, ""}, BackupSummary{}, true},
		{"GetBackupCountUnexpectedResult", fields{db1, ""}, BackupSummary{}, true},
		{"BackupCountScanError", fields{db1, ""}, BackupSummary{}, true},
		{"BackupCountDbError", fields{db1, ""}, BackupSummary{}, true},
		{"BackupCatalogEntryCountScanError", fields{db1, ""}, BackupSummary{}, true},
		{"BackupCatalogEntryCountDbError", fields{db1, ""}, BackupSummary{}, true},
	}
	for _, tt := range tests {
		/*Set up per case mocking*/
		switch tt.name {
		case "Good01":
			rows1 := mock.NewRows([]string{"COUNT"}).AddRow(100)
			rows2 := mock.NewRows([]string{"COUNT", "ENTRY_TYPE_NAME"})
			rows2.AddRow(10, "complete data backup")
			rows2.AddRow(90, "log backup")
			rows3 := mock.NewRows([]string{"TYPES", "BYTES"})
			rows3.AddRow("complete data backup", 1024000)
			rows3.AddRow("log backup", 512000)
			rows4 := mock.NewRows([]string{"ENTRY_TYPE_NAME", "UTC_START_NAME"})
			rows4.AddRow("complete data backup", genTime)
			rows4.AddRow("log backup", genTime)
			rows5 := mock.NewRows([]string{"BF.BACKUP_SIZE"}).AddRow(10240)
			/*Now do the sequencing*/
			mock.ExpectQuery(q_GetBackupCatalogEntryCount).WillReturnRows(rows1)
			mock.ExpectQuery(q_GetBackupCount).WillReturnRows(rows2)
			mock.ExpectQuery(q_GetBackupSizes).WillReturnRows(rows3)
			mock.ExpectQuery(q_GetOldestBackups).WillReturnRows(rows4)
			mock.ExpectQuery(q_GetBackupCatalogSize).WillReturnRows(rows5)
		case "Good02":
			rows1 := mock.NewRows([]string{"COUNT"}).AddRow(60)
			rows2 := mock.NewRows([]string{"COUNT", "ENTRY_TYPE_NAME"})
			rows2.AddRow(10, "complete data backup")
			rows2.AddRow(10, "incremental data backup")
			rows2.AddRow(10, "differential data backup")
			rows2.AddRow(10, "log backup")
			rows2.AddRow(10, "log missing")
			rows2.AddRow(10, "data snapshot")
			rows3 := mock.NewRows([]string{"TYPES", "BYTES"})
			rows3.AddRow("complete data backup", 1024)
			rows3.AddRow("incremental data backup", 1024)
			rows3.AddRow("differential data backup", 1024)
			rows3.AddRow("log backup", 1024)
			rows3.AddRow("log missing", 1024)
			rows3.AddRow("data snapshot", 1024)
			rows4 := mock.NewRows([]string{"ENTRY_TYPE_NAME", "UTC_START_NAME"})
			rows4.AddRow("complete data backup", genTime)
			rows4.AddRow("log backup", genTime)
			rows5 := mock.NewRows([]string{"BF.BACKUP_SIZE"}).AddRow(10240)
			/*Now do the sequencing*/
			mock.ExpectQuery(q_GetBackupCatalogEntryCount).WillReturnRows(rows1)
			mock.ExpectQuery(q_GetBackupCount).WillReturnRows(rows2)
			mock.ExpectQuery(q_GetBackupSizes).WillReturnRows(rows3)
			mock.ExpectQuery(q_GetOldestBackups).WillReturnRows(rows4)
			mock.ExpectQuery(q_GetBackupCatalogSize).WillReturnRows(rows5)
		case "OldestBackupUnexpectedResult":
			rows1 := mock.NewRows([]string{"COUNT"}).AddRow(100)
			rows2 := mock.NewRows([]string{"COUNT", "ENTRY_TYPE_NAME"})
			rows2.AddRow(10, "complete data backup")
			rows2.AddRow(90, "log backup")
			rows3 := mock.NewRows([]string{"TYPES", "BYTES"})
			rows3.AddRow("complete data backup", 1024000)
			rows3.AddRow("log backup", 512000)
			rows4 := mock.NewRows([]string{"ENTRY_TYPE_NAME", "UTC_START_NAME"})
			rows4.AddRow("not an expected field", genTime)
			rows4.AddRow("log backup", genTime)
			/*Now do the sequencing*/
			mock.ExpectQuery(q_GetBackupCatalogEntryCount).WillReturnRows(rows1)
			mock.ExpectQuery(q_GetBackupCount).WillReturnRows(rows2)
			mock.ExpectQuery(q_GetBackupSizes).WillReturnRows(rows3)
			mock.ExpectQuery(q_GetOldestBackups).WillReturnRows(rows4)
		case "OldestBackupScanError":
			rows1 := mock.NewRows([]string{"COUNT"}).AddRow(100)
			rows2 := mock.NewRows([]string{"COUNT", "ENTRY_TYPE_NAME"})
			rows2.AddRow(10, "complete data backup")
			rows2.AddRow(90, "log backup")
			rows3 := mock.NewRows([]string{"TYPES", "BYTES"})
			rows3.AddRow("complete data backup", 1024000)
			rows3.AddRow("log backup", 512000)
			rows4 := mock.NewRows([]string{"ENTRY_TYPE_NAME", "UTC_START_NAME"})
			rows4.AddRow("not an expected field", "a string")
			rows4.AddRow("log backup", 0.1)
			/*Now do the sequencing*/
			mock.ExpectQuery(q_GetBackupCatalogEntryCount).WillReturnRows(rows1)
			mock.ExpectQuery(q_GetBackupCount).WillReturnRows(rows2)
			mock.ExpectQuery(q_GetBackupSizes).WillReturnRows(rows3)
			mock.ExpectQuery(q_GetOldestBackups).WillReturnRows(rows4)
		case "OldestBackupDbError":
			rows1 := mock.NewRows([]string{"COUNT"}).AddRow(100)
			rows2 := mock.NewRows([]string{"COUNT", "ENTRY_TYPE_NAME"})
			rows2.AddRow(10, "complete data backup")
			rows2.AddRow(90, "log backup")
			rows3 := mock.NewRows([]string{"TYPES", "BYTES"})
			rows3.AddRow("complete data backup", 1024000)
			rows3.AddRow("log backup", 512000)
			/*Now do the sequencing*/
			mock.ExpectQuery(q_GetBackupCatalogEntryCount).WillReturnRows(rows1)
			mock.ExpectQuery(q_GetBackupCount).WillReturnRows(rows2)
			mock.ExpectQuery(q_GetBackupSizes).WillReturnRows(rows3)
			mock.ExpectQuery(q_GetOldestBackups).WillReturnError(fmt.Errorf("DbError"))
		case "BackupCatalogSizeDbError":
			rows1 := mock.NewRows([]string{"COUNT"}).AddRow(100)
			rows2 := mock.NewRows([]string{"COUNT", "ENTRY_TYPE_NAME"})
			rows2.AddRow(10, "complete data backup")
			rows2.AddRow(90, "log backup")
			rows3 := mock.NewRows([]string{"TYPES", "BYTES"})
			rows3.AddRow("complete data backup", 1024000)
			rows3.AddRow("log backup", 512000)
			rows4 := mock.NewRows([]string{"ENTRY_TYPE_NAME", "UTC_START_NAME"})
			rows4.AddRow("complete data backup", genTime)
			rows4.AddRow("log backup", genTime)
			/*Now do the sequencing*/
			mock.ExpectQuery(q_GetBackupCatalogEntryCount).WillReturnRows(rows1)
			mock.ExpectQuery(q_GetBackupCount).WillReturnRows(rows2)
			mock.ExpectQuery(q_GetBackupSizes).WillReturnRows(rows3)
			mock.ExpectQuery(q_GetOldestBackups).WillReturnRows(rows4)
			mock.ExpectQuery(q_GetBackupCatalogSize).WillReturnError(fmt.Errorf("DbError"))
		case "BackupSizeUnexpectedResult":
			rows1 := mock.NewRows([]string{"COUNT"}).AddRow(100)
			rows2 := mock.NewRows([]string{"COUNT", "ENTRY_TYPE_NAME"})
			rows2.AddRow(10, "complete data backup")
			rows2.AddRow(90, "log backup")
			rows3 := mock.NewRows([]string{"TYPES", "BYTES"})
			rows3.AddRow("complete data backup", 1024000)
			rows3.AddRow("unexpected result", 512000)
			/*Now do the sequencing*/
			mock.ExpectQuery(q_GetBackupCatalogEntryCount).WillReturnRows(rows1)
			mock.ExpectQuery(q_GetBackupCount).WillReturnRows(rows2)
			mock.ExpectQuery(q_GetBackupSizes).WillReturnRows(rows3)
		case "BackupSizeScanError":
			rows1 := mock.NewRows([]string{"COUNT"}).AddRow(100)
			rows2 := mock.NewRows([]string{"COUNT", "ENTRY_TYPE_NAME"})
			rows2.AddRow(10, "complete data backup")
			rows2.AddRow(90, "log backup")
			rows3 := mock.NewRows([]string{"TYPES", "BYTES"})
			rows3.AddRow("log backup", "-5")
			/*Now do the sequencing*/
			mock.ExpectQuery(q_GetBackupCatalogEntryCount).WillReturnRows(rows1)
			mock.ExpectQuery(q_GetBackupCount).WillReturnRows(rows2)
			mock.ExpectQuery(q_GetBackupSizes).WillReturnRows(rows3)
		case "BackupSizeDbError":
			rows1 := mock.NewRows([]string{"COUNT"}).AddRow(100)
			rows2 := mock.NewRows([]string{"COUNT", "ENTRY_TYPE_NAME"})
			rows2.AddRow(10, "complete data backup")
			rows2.AddRow(90, "log backup")
			/*Now do the sequencing*/
			mock.ExpectQuery(q_GetBackupCatalogEntryCount).WillReturnRows(rows1)
			mock.ExpectQuery(q_GetBackupCount).WillReturnRows(rows2)
			mock.ExpectQuery(q_GetBackupSizes).WillReturnError(fmt.Errorf("DbError"))
		case "GetBackupCountUnexpectedResult":
			rows1 := mock.NewRows([]string{"COUNT"}).AddRow(100)
			rows2 := mock.NewRows([]string{"COUNT", "ENTRY_TYPE_NAME"})
			rows2.AddRow(10, "complete data backup")
			rows2.AddRow(90, "super unexpexted log backup")
			/*Now do the sequencing*/
			mock.ExpectQuery(q_GetBackupCatalogEntryCount).WillReturnRows(rows1)
			mock.ExpectQuery(q_GetBackupCount).WillReturnRows(rows2)
		case "BackupCountScanError":
			rows1 := mock.NewRows([]string{"COUNT"}).AddRow(100)
			rows2 := mock.NewRows([]string{"COUNT", "ENTRY_TYPE_NAME"})
			rows2.AddRow("-10", "complete data backup")
			/*Now do the sequencing*/
			mock.ExpectQuery(q_GetBackupCatalogEntryCount).WillReturnRows(rows1)
			mock.ExpectQuery(q_GetBackupCount).WillReturnRows(rows2)
		case "BackupCountDbError":
			rows1 := mock.NewRows([]string{"COUNT"}).AddRow(100)
			/*Now do the sequencing*/
			mock.ExpectQuery(q_GetBackupCatalogEntryCount).WillReturnRows(rows1)
			mock.ExpectQuery(q_GetBackupCount).WillReturnError(fmt.Errorf("DbError"))
		case "BackupCatalogEntryCountScanError":
			rows1 := mock.NewRows([]string{"COUNT"}).AddRow("-1")
			/*Now do the sequencing*/
			mock.ExpectQuery(q_GetBackupCatalogEntryCount).WillReturnRows(rows1)
		case "BackupCatalogEntryCountDbError":
			mock.ExpectQuery(q_GetBackupCatalogEntryCount).WillReturnError(fmt.Errorf("DbError"))
		default:
			fmt.Printf("No test case matched for %s\n", tt.name)
			t.Errorf("No test case matched")
		}
		t.Run(tt.name, func(t *testing.T) {
			h := &hanaUtilClient{
				db:  tt.fields.db,
				dsn: tt.fields.dsn,
			}
			got, err := h.GetBackupSummary()
			if (err != nil) != tt.wantErr {
				t.Errorf("hanaUtilClient.GetBackupSummary() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("hanaUtilClient.GetBackupSummary() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_hanaUtilClient_GetStatServerAlerts(t *testing.T) {
	/*Test Setup*/
	/*Mock DB*/
	db1, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Errorf("an error '%s' was not expected when opening mock database connection", err)
	}
	defer db1.Close()
	type fields struct {
		db  *sql.DB
		dsn string
	}
	type args struct {
		days uint
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    uint
		wantErr bool
	}{
		{"Good", fields{db1, ""}, args{42}, 99, false},
		{"NoRows", fields{db1, ""}, args{50}, 0, true},
	}
	for _, tt := range tests {
		/*Set up per case mocking*/
		switch tt.name {
		case "Good":
			rows1 := mock.NewRows([]string{"COUNT"}).AddRow(99)
			mock.ExpectQuery(f_GetStatServerAlerts(42)).WillReturnRows(rows1)
		case "NoRows":
			rows1 := mock.NewRows([]string{"COUNT"})
			mock.ExpectQuery(f_GetStatServerAlerts(42)).WillReturnRows(rows1)
		}
		t.Run(tt.name, func(t *testing.T) {
			h := &hanaUtilClient{
				db:  tt.fields.db,
				dsn: tt.fields.dsn,
			}
			got, err := h.GetStatServerAlerts(tt.args.days)
			if (err != nil) != tt.wantErr {
				t.Errorf("hanaUtilClient.GetStatServerAlerts() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("hanaUtilClient.GetStatServerAlerts() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_hanaUtilClient_GetLogSegmentStats(t *testing.T) {
	/*Test Setup*/
	/*Mock DB*/
	db1, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Errorf("an error '%s' was not expected when opening mock database connection", err)
	}
	defer db1.Close()
	type fields struct {
		db  *sql.DB
		dsn string
	}
	tests := []struct {
		name    string
		fields  fields
		want    LogSegmentsStats
		wantErr bool
	}{
		{"Good", fields{db1, ""}, LogSegmentsStats{10, 10240, 50, 51200}, false},
		{"NoRows", fields{db1, ""}, LogSegmentsStats{}, false},
		{"DbError", fields{db1, ""}, LogSegmentsStats{}, true},
		{"ScanError", fields{db1, ""}, LogSegmentsStats{}, true},
		{"UnexpectedReturn", fields{db1, ""}, LogSegmentsStats{}, true},
	}
	for _, tt := range tests {
		/*Set up per case mocking*/
		switch tt.name {
		case "Good":
			rows1 := mock.NewRows([]string{"STATE", "SEGMENTS", "BYTES"})
			rows1.AddRow("Free", 10, 10240)
			rows1.AddRow("NonFree", 50, 51200)
			mock.ExpectQuery(q_GetLogSegmentStats).WillReturnRows(rows1)
		case "NoRows":
			rows1 := mock.NewRows([]string{"STATE", "SEGMENTS", "BYTES"})
			mock.ExpectQuery(q_GetLogSegmentStats).WillReturnRows(rows1)
		case "DbError":
			mock.ExpectQuery(q_GetLogSegmentStats).WillReturnError(fmt.Errorf("DbError"))
		case "ScanError":
			rows1 := mock.NewRows([]string{"STATE", "SEGMENTS", "BYTES"})
			rows1.AddRow("Free", 10, "10240.12")
			rows1.AddRow("NonFree", 50, 51200)
			mock.ExpectQuery(q_GetLogSegmentStats).WillReturnRows(rows1)
		case "UnexpectedReturn":
			rows1 := mock.NewRows([]string{"STATE", "SEGMENTS", "BYTES"})
			rows1.AddRow("NotExpected", 10, 10240)
			mock.ExpectQuery(q_GetLogSegmentStats).WillReturnRows(rows1)
		default:
			fmt.Printf("No test case matched for %s\n", tt.name)
			t.Errorf("No test case matched")
		}
		t.Run(tt.name, func(t *testing.T) {
			h := &hanaUtilClient{
				db:  tt.fields.db,
				dsn: tt.fields.dsn,
			}
			got, err := h.GetLogSegmentStats()
			if (err != nil) != tt.wantErr {
				t.Errorf("hanaUtilClient.GetLogSegmentStats() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("hanaUtilClient.GetLogSegmentStats() = %v, want %v", got, tt.want)
			}
		})
	}
}
