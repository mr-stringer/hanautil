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
			h := &HanaUtilClient{
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
			mock.ExpectQuery(f_GetTraceFiles(7)).WillReturnError(fmt.Errorf("dbError"))
		case tt.name == "ScanError":
			row := sqlmock.NewRows([]string{"HOST", "FILE_NAME", "FILE_SIZE", "FILE_MTIME"})
			row.AddRow("hana01", "nameserver_hana01.30001.000.trc", 64000.1, genTime)
			mock.ExpectQuery(f_GetTraceFiles(7)).WillReturnRows(row)
		default:
			fmt.Println("No test case matched")
			t.Errorf("No test case matched")

		}
		t.Run(tt.name, func(t *testing.T) {
			h := &HanaUtilClient{
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
			100, 10, 90, 30, 0, 0, 0, 1024000, 512000, 0, 0, 0, 0, 10240, genTime, genTime, genTime}, false},
		{"Good02", fields{db1, ""}, BackupSummary{
			60, 10, 10, 10, 10, 10, 10, 1024, 1024, 1024, 1024, 1024, 1024, 10240, genTime, genTime, genTime}, false},
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
		{"CurrentTimeError", fields{db1, ""}, BackupSummary{}, true},
	}
	for _, tt := range tests {
		/*Set up per case mocking*/
		switch tt.name {
		case "Good01":
			rows1 := mock.NewRows([]string{"COUNT"}).AddRow(100)
			rows2 := mock.NewRows([]string{"COUNT", "ENTRY_TYPE_NAME"})
			rows2.AddRow(10, "complete data backup")
			rows2.AddRow(90, "log backup")
			rows2.AddRow(30, "incremental data backup")
			rows3 := mock.NewRows([]string{"TYPES", "BYTES"})
			rows3.AddRow("complete data backup", 1024000)
			rows3.AddRow("log backup", 512000)
			rows4 := mock.NewRows([]string{"ENTRY_TYPE_NAME", "UTC_START_NAME"})
			rows4.AddRow("complete data backup", genTime)
			rows4.AddRow("log backup", genTime)
			rows5 := mock.NewRows([]string{"BF.BACKUP_SIZE"}).AddRow(10240)
			rows6 := mock.NewRows([]string{"CURRENT_TIME}"}).AddRow(genTime)
			/*Now do the sequencing*/
			mock.ExpectQuery(q_GetBackupCatalogEntryCount).WillReturnRows(rows1)
			mock.ExpectQuery(q_GetBackupCount).WillReturnRows(rows2)
			mock.ExpectQuery(q_GetBackupSizes).WillReturnRows(rows3)
			mock.ExpectQuery(q_GetOldestBackups).WillReturnRows(rows4)
			mock.ExpectQuery(q_GetBackupCatalogSize).WillReturnRows(rows5)
			mock.ExpectQuery(q_GetDbCurrentTime).WillReturnRows(rows6)
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
			rows6 := mock.NewRows([]string{"CURRENT_TIME}"}).AddRow(genTime)
			/*Now do the sequencing*/
			mock.ExpectQuery(q_GetBackupCatalogEntryCount).WillReturnRows(rows1)
			mock.ExpectQuery(q_GetBackupCount).WillReturnRows(rows2)
			mock.ExpectQuery(q_GetBackupSizes).WillReturnRows(rows3)
			mock.ExpectQuery(q_GetOldestBackups).WillReturnRows(rows4)
			mock.ExpectQuery(q_GetBackupCatalogSize).WillReturnRows(rows5)
			mock.ExpectQuery(q_GetDbCurrentTime).WillReturnRows(rows6)
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
			mock.ExpectQuery(q_GetOldestBackups).WillReturnError(fmt.Errorf("dbError"))
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
			mock.ExpectQuery(q_GetBackupCatalogSize).WillReturnError(fmt.Errorf("dbError"))
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
			mock.ExpectQuery(q_GetBackupSizes).WillReturnError(fmt.Errorf("dbError"))
		case "GetBackupCountUnexpectedResult":
			rows1 := mock.NewRows([]string{"COUNT"}).AddRow(100)
			rows2 := mock.NewRows([]string{"COUNT", "ENTRY_TYPE_NAME"})
			rows2.AddRow(10, "complete data backup")
			rows2.AddRow(90, "super unexpected log backup")
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
			mock.ExpectQuery(q_GetBackupCount).WillReturnError(fmt.Errorf("dbError"))
		case "BackupCatalogEntryCountScanError":
			rows1 := mock.NewRows([]string{"COUNT"}).AddRow("-1")
			/*Now do the sequencing*/
			mock.ExpectQuery(q_GetBackupCatalogEntryCount).WillReturnRows(rows1)
		case "BackupCatalogEntryCountDbError":
			mock.ExpectQuery(q_GetBackupCatalogEntryCount).WillReturnError(fmt.Errorf("dbError"))
		case "CurrentTimeError":
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
			rows6 := mock.NewRows([]string{"CURRENT_TIME}"})
			rows6.AddRow(genTime)
			/*Now do the sequencing*/
			mock.ExpectQuery(q_GetBackupCatalogEntryCount).WillReturnRows(rows1)
			mock.ExpectQuery(q_GetBackupCount).WillReturnRows(rows2)
			mock.ExpectQuery(q_GetBackupSizes).WillReturnRows(rows3)
			mock.ExpectQuery(q_GetOldestBackups).WillReturnRows(rows4)
			mock.ExpectQuery(q_GetBackupCatalogSize).WillReturnRows(rows5)
			mock.ExpectQuery(q_GetDbCurrentTime).WillReturnError(fmt.Errorf("dbError"))
		default:
			fmt.Printf("No test case matched for %s\n", tt.name)
			t.Errorf("No test case matched")
		}
		t.Run(tt.name, func(t *testing.T) {
			h := &HanaUtilClient{
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
			h := &HanaUtilClient{
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
			mock.ExpectQuery(q_GetLogSegmentStats).WillReturnError(fmt.Errorf("dbError"))
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
			h := &HanaUtilClient{
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

func TestHanaUtilClient_GetBackupSummaryBeforeBackupID(t *testing.T) {
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
		b string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    BackupSummary
		wantErr bool
	}{
		{"Good01", fields{db1, ""}, args{"123"}, BackupSummary{
			100, 10, 90, 0, 0, 0, 0, 1024000, 512000, 0, 0, 0, 0, 10240, genTime, genTime, genTime}, false},
		{"Bad", fields{db1, ""}, args{"123"}, BackupSummary{}, true},
	}
	for _, tt := range tests {
		/*per case mocking*/
		switch {
		case tt.name == "Good01":
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
			rows6 := mock.NewRows([]string{"CURRENT_TIME}"}).AddRow(genTime)
			/*Now do the sequencing*/
			mock.ExpectQuery(f_GetBackupCatalogEntryCountBeforeID(tt.args.b)).WillReturnRows(rows1)
			mock.ExpectQuery(f_GetBackupCountBeforeID(tt.args.b)).WillReturnRows(rows2)
			mock.ExpectQuery(f_GetBackupSizesBeforeId(tt.args.b)).WillReturnRows(rows3)
			mock.ExpectQuery(q_GetOldestBackups).WillReturnRows(rows4)
			mock.ExpectQuery(q_GetBackupCatalogSize).WillReturnRows(rows5)
			mock.ExpectQuery(q_GetDbCurrentTime).WillReturnRows(rows6)
		case tt.name == "Bad":
			mock.ExpectQuery(f_GetBackupCatalogEntryCountBeforeID(tt.args.b)).WillReturnError(fmt.Errorf("DB error"))
		default:
			fmt.Printf("No test case matched for %s\n", tt.name)
			t.Errorf("No test case matched")

		}
		t.Run(tt.name, func(t *testing.T) {
			h := &HanaUtilClient{
				db:  tt.fields.db,
				dsn: tt.fields.dsn,
			}
			got, err := h.GetBackupSummaryBeforeBackupID(tt.args.b)
			if (err != nil) != tt.wantErr {
				t.Errorf("HanaUtilClient.GetBackupSummaryBeforeBackupID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HanaUtilClient.GetBackupSummaryBeforeBackupID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHanaUtilClient_GetFullBackupId(t *testing.T) {
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
		days int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		{"Good01", fields{db1, ""}, args{14}, "123456789", false},
		{"DbError", fields{db1, ""}, args{10}, "", true},
	}
	for _, tt := range tests {
		/*per case mocking*/
		switch {
		case tt.name == "Good01":
			r1 := mock.NewRows([]string{"BACKUP_ID"})
			r1.AddRow("123456789")
			// expect
			mock.ExpectQuery(q_GetLatestFullBackupID(uint(tt.args.days))).WillReturnRows(r1)
		case tt.name == "DbError":
			mock.ExpectQuery(q_GetLatestFullBackupID(uint(tt.args.days))).WillReturnError(fmt.Errorf("dbError"))
		default:
			fmt.Printf("No test case matched for %s\n", tt.name)
			t.Errorf("No test case matched")
		}
		t.Run(tt.name, func(t *testing.T) {
			h := &HanaUtilClient{
				db:  tt.fields.db,
				dsn: tt.fields.dsn,
			}
			got, err := h.GetFullBackupId(tt.args.days)
			if (err != nil) != tt.wantErr {
				t.Errorf("HanaUtilClient.GetFullBackupId() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("HanaUtilClient.GetFullBackupId() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHanaUtilClient_GetDataFragStats(t *testing.T) {
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
		want    []DataVolumeFragStats
		wantErr bool
	}{
		{"Good01", fields{db1, ""}, []DataVolumeFragStats{
			{"hdb01", 30015, "indexserver", 1024000, 819200, 20},
		}, false},
		{"Good02", fields{db1, ""}, []DataVolumeFragStats{
			{"hdb01", 30040, "indexserver", 2190433320960, 1697585823744, 22.5},
			{"hdb01", 30041, "xsengine", 10737418240, 10200547328, 5},
		}, false},
		{"DbError", fields{db1, ""}, nil, true},
		{"ScanError", fields{db1, ""}, nil, true},
	}
	for _, tt := range tests {
		/*per case mocks*/
		switch {
		case tt.name == "Good01":
			r1 := mock.NewRows([]string{"HOST", "PORT", "SERVICE_NAME", "TOTAL_SIZE", "USED_SIZE"})
			r1.AddRow("hdb01", 30015, "indexserver", 1024000, 819200)
			// expect
			mock.ExpectQuery(q_GetDataDefrag).WillReturnRows(r1)
		case tt.name == "Good02":
			r1 := mock.NewRows([]string{"HOST", "PORT", "SERVICE_NAME", "TOTAL_SIZE", "USED_SIZE"})
			r1.AddRow("hdb01", 30040, "indexserver", 2190433320960, 1697585823744)
			r1.AddRow("hdb01", 30041, "xsengine", 10737418240, 10200547328)
			mock.ExpectQuery(q_GetDataDefrag).WillReturnRows(r1)
		case tt.name == "DbError":
			mock.ExpectQuery(q_GetDataDefrag).WillReturnError(fmt.Errorf("dbError"))
		case tt.name == "ScanError":
			r1 := mock.NewRows([]string{"HOST", "PORT", "SERVICE_NAME", "TOTAL_SIZE", "USED_SIZE"})
			r1.AddRow("hdb01", 30040, "indexserver", 2190433320960, 1697585823744.5)
			mock.ExpectQuery(q_GetDataDefrag).WillReturnRows(r1)

		}
		t.Run(tt.name, func(t *testing.T) {
			h := &HanaUtilClient{
				db:  tt.fields.db,
				dsn: tt.fields.dsn,
			}
			got, err := h.GetDataFragStats()
			if (err != nil) != tt.wantErr {
				t.Errorf("HanaUtilClient.GetDataFragStats() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HanaUtilClient.GetDataFragStats() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHanaUtilClient_GetVolFragStats(t *testing.T) {
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
		host string
		port uint
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *DataVolumeFragStats
		wantErr bool
	}{
		{"Good01", fields{db1, ""}, args{"demo", 30003}, &DataVolumeFragStats{"demo", 30003, "indexserver", 1000000000, 900000000, 10.0}, false},
		{"DbError", fields{db1, ""}, args{"hdbsrv01", 35000}, nil, true},
		{"TooManyResults", fields{db1, ""}, args{"demo", 30003}, nil, true},
		{"ScanError", fields{db1, ""}, args{"myhost", 36903}, nil, true},
	}
	for _, tt := range tests {
		//per case mocking
		switch {
		case tt.name == "Good01":
			r1 := mock.NewRows([]string{"HOST", "PORT", "SERVICE_NAME", "TOTAL_SIZE", "USED_SIZE"})
			r1.AddRow("demo", 30003, "indexserver", 1000000000, 900000000)
			mock.ExpectQuery(q_GetDataVolume(tt.args.host, tt.args.port)).WillReturnRows(r1)
		case tt.name == "DbError":
			mock.ExpectQuery(q_GetDataVolume(tt.args.host, tt.args.port)).WillReturnError(fmt.Errorf("DbError"))
		case tt.name == "TooManyResults":
			r1 := mock.NewRows([]string{"HOST", "PORT", "SERVICE_NAME", "TOTAL_SIZE", "USED_SIZE"})
			r1.AddRow("demo", 30003, "indexserver", 1000000000, 900000000)
			r1.AddRow("demo", 30003, "indexserver", 9000000000, 5523434543)
			mock.ExpectQuery(q_GetDataVolume(tt.args.host, tt.args.port)).WillReturnRows(r1)
		case tt.name == "ScanError":
			r1 := mock.NewRows([]string{"HOST", "PORT", "SERVICE_NAME", "TOTAL_SIZE", "USED_SIZE"})
			r1.AddRow("myhost", "NAN", "indexserver", 1000000000, 90000000.10)
			mock.ExpectQuery(q_GetDataVolume(tt.args.host, tt.args.port)).WillReturnRows(r1)
		}
		t.Run(tt.name, func(t *testing.T) {
			h := &HanaUtilClient{
				db:  tt.fields.db,
				dsn: tt.fields.dsn,
			}
			got, err := h.GetVolFragStats(tt.args.host, tt.args.port)
			if (err != nil) != tt.wantErr {
				t.Errorf("HanaUtilClient.GetVolFragStats() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HanaUtilClient.GetVolFragStats() = %v, want %v", got, tt.want)
			}
		})
	}
}
