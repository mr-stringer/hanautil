package hanautil

import (
	"database/sql"
	"fmt"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func Test_hanaUtilClient_TruncateBackupCatalog(t *testing.T) {
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
		days     int
		complete bool
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    TruncateStats
		wantErr bool
	}{
		{"Good", fields{db1, ""}, args{28, false}, TruncateStats{100, 0}, false},
		{"GoodPartialDelete", fields{db1, ""}, args{28, false}, TruncateStats{99, 0}, false},
		{"GoodNoDelete", fields{db1, ""}, args{31, false}, TruncateStats{100, 0}, false},
		{"GoodComplete", fields{db1, ""}, args{31, true}, TruncateStats{99, 99999}, false},
		{"GoodCompleteNoDelete", fields{db1, ""}, args{31, true}, TruncateStats{75, 555444}, false},
		{"GoodCompletePartialDelete", fields{db1, ""}, args{28, true}, TruncateStats{1000, 1000000}, false},
		{"2ndGetTruncateDbError", fields{db1, ""}, args{28, false}, TruncateStats{}, true},
		{"2ndGetTruncateScanError", fields{db1, ""}, args{28, false}, TruncateStats{}, true},
		{"TruncateDbError", fields{db1, ""}, args{90, false}, TruncateStats{}, true},
		{"TruncateCompleteDbError", fields{db1, ""}, args{90, true}, TruncateStats{}, true},
		{"1stGetTruncateDbError", fields{db1, ""}, args{60, false}, TruncateStats{}, true},
		{"1stGetTruncateScanError", fields{db1, ""}, args{60, false}, TruncateStats{}, true},
		{"GetBackupIdScanError", fields{db1, ""}, args{14, false}, TruncateStats{}, true},
		{"GetBackupIdDbError", fields{db1, ""}, args{14, false}, TruncateStats{}, true},
	}

	for _, tt := range tests {
		/*Set up per case mocking*/
		switch tt.name {
		case "Good":
			var backupID string = "1038347234"
			rows1 := mock.NewRows([]string{"BACKUP_ID"}).AddRow(backupID)
			rows2 := mock.NewRows([]string{"FILES", "BACKUP_SIZE"})
			rows2.AddRow("100", "1024000")
			rows3 := mock.NewRows([]string{"FILES", "BACKUP_SIZE"})
			rows3.AddRow("0", "0")
			mock.ExpectQuery(q_GetLatestFullBackupID(uint(tt.args.days))).WillReturnRows(rows1)
			mock.ExpectQuery(f_GetTruncateData(backupID)).WillReturnRows(rows2)
			mock.ExpectExec(f_GetBackupDelete(backupID)).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(f_GetTruncateData(backupID)).WillReturnRows(rows3)
		case "GoodComplete":
			var backupID string = "1038347234"
			rows1 := mock.NewRows([]string{"BACKUP_ID"}).AddRow(backupID)
			rows2 := mock.NewRows([]string{"FILES", "BACKUP_SIZE"})
			rows2.AddRow("100", "100000")
			rows3 := mock.NewRows([]string{"FILES", "BACKUP_SIZE"})
			rows3.AddRow("1", "1")
			mock.ExpectQuery(q_GetLatestFullBackupID(uint(tt.args.days))).WillReturnRows(rows1)
			mock.ExpectQuery(f_GetTruncateData(backupID)).WillReturnRows(rows2)
			mock.ExpectExec(f_GetBackupDeleteComplete(backupID)).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(f_GetTruncateData(backupID)).WillReturnRows(rows3)
		case "GoodPartialDelete":
			var backupID string = "34897345745"
			rows1 := mock.NewRows([]string{"BACKUP_ID"}).AddRow(backupID)
			rows2 := mock.NewRows([]string{"FILES", "BACKUP_SIZE"})
			rows2.AddRow("100", "100000")
			rows3 := mock.NewRows([]string{"FILES", "BACKUP_SIZE"})
			rows3.AddRow("1", "1")
			mock.ExpectQuery(q_GetLatestFullBackupID(uint(tt.args.days))).WillReturnRows(rows1)
			mock.ExpectQuery(f_GetTruncateData(backupID)).WillReturnRows(rows2)
			mock.ExpectExec(f_GetBackupDelete(backupID)).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(f_GetTruncateData(backupID)).WillReturnRows(rows3)
		case "GoodNoDelete":
			var backupID string = "1983456873456"
			rows1 := mock.NewRows([]string{"BACKUP_ID"}).AddRow(backupID)
			rows2 := mock.NewRows([]string{"FILES", "BACKUP_SIZE"})
			rows2.AddRow("100", "100000")
			rows3 := mock.NewRows([]string{"FILES", "BACKUP_SIZE"})
			rows3.AddRow("100", "0")
			mock.ExpectQuery(q_GetLatestFullBackupID(uint(tt.args.days))).WillReturnRows(rows1)
			mock.ExpectQuery(f_GetTruncateData(backupID)).WillReturnRows(rows2)
			mock.ExpectExec(f_GetBackupDelete(backupID)).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(f_GetTruncateData(backupID)).WillReturnRows(rows3)
		case "GoodCompletePartialDelete":
			var backupID string = "1038347234"
			rows1 := mock.NewRows([]string{"BACKUP_ID"}).AddRow(backupID)
			rows2 := mock.NewRows([]string{"FILES", "BACKUP_SIZE"})
			rows2.AddRow("1010", "1100000")
			rows3 := mock.NewRows([]string{"FILES", "BACKUP_SIZE"})
			rows3.AddRow("10", "100000")
			mock.ExpectQuery(q_GetLatestFullBackupID(uint(tt.args.days))).WillReturnRows(rows1)
			mock.ExpectQuery(f_GetTruncateData(backupID)).WillReturnRows(rows2)
			mock.ExpectExec(f_GetBackupDeleteComplete(backupID)).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(f_GetTruncateData(backupID)).WillReturnRows(rows3)
		case "GoodCompleteNoDelete":
			var backupID string = "98374569874"
			rows1 := mock.NewRows([]string{"BACKUP_ID"}).AddRow(backupID)
			rows2 := mock.NewRows([]string{"FILES", "BACKUP_SIZE"})
			rows2.AddRow("75", "555444")
			rows3 := mock.NewRows([]string{"FILES", "BACKUP_SIZE"})
			rows3.AddRow("75", "555444")
			mock.ExpectQuery(q_GetLatestFullBackupID(uint(tt.args.days))).WillReturnRows(rows1)
			mock.ExpectQuery(f_GetTruncateData(backupID)).WillReturnRows(rows2)
			mock.ExpectExec(f_GetBackupDeleteComplete(backupID)).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(f_GetTruncateData(backupID)).WillReturnRows(rows3)
		case "2ndGetTruncateDbError":
			var backupID string = "34897345745"
			rows1 := mock.NewRows([]string{"BACKUP_ID"}).AddRow(backupID)
			rows2 := mock.NewRows([]string{"FILES", "BACKUP_SIZE"})
			rows2.AddRow("100", "100000")
			mock.ExpectQuery(q_GetLatestFullBackupID(uint(tt.args.days))).WillReturnRows(rows1)
			mock.ExpectQuery(f_GetTruncateData(backupID)).WillReturnRows(rows2)
			mock.ExpectExec(f_GetBackupDelete(backupID)).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(f_GetTruncateData(backupID)).WillReturnError(fmt.Errorf("DbError"))
		case "2ndGetTruncateScanError":
			var backupID string = "34897345745"
			rows1 := mock.NewRows([]string{"BACKUP_ID"}).AddRow(backupID)
			rows2 := mock.NewRows([]string{"FILES", "BACKUP_SIZE"})
			rows2.AddRow("100", "100000")
			rows3 := mock.NewRows([]string{"FILES", "BACKUP_SIZE"})
			rows3.AddRow("Not an expected value", true)
			mock.ExpectQuery(q_GetLatestFullBackupID(uint(tt.args.days))).WillReturnRows(rows1)
			mock.ExpectQuery(f_GetTruncateData(backupID)).WillReturnRows(rows2)
			mock.ExpectExec(f_GetBackupDelete(backupID)).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(f_GetTruncateData(backupID)).WillReturnError(fmt.Errorf("DbError"))
		case "TruncateDbError":
			var backupID string = "1038347234"
			rows1 := mock.NewRows([]string{"BACKUP_ID"}).AddRow(backupID)
			rows2 := mock.NewRows([]string{"FILES", "BACKUP_SIZE"})
			rows2.AddRow("100", "1024000")
			mock.ExpectQuery(q_GetLatestFullBackupID(uint(tt.args.days))).WillReturnRows(rows1)
			mock.ExpectQuery(f_GetTruncateData(backupID)).WillReturnRows(rows2)
			mock.ExpectExec(f_GetBackupDelete(backupID)).WillReturnError(fmt.Errorf("DbError"))
		case "TruncateCompleteDbError":
			var backupID string = "1038347234"
			rows1 := mock.NewRows([]string{"BACKUP_ID"}).AddRow(backupID)
			rows2 := mock.NewRows([]string{"FILES", "BACKUP_SIZE"})
			rows2.AddRow("100", "1024000")
			mock.ExpectQuery(q_GetLatestFullBackupID(uint(tt.args.days))).WillReturnRows(rows1)
			mock.ExpectQuery(f_GetTruncateData(backupID)).WillReturnRows(rows2)
			mock.ExpectExec(f_GetBackupDeleteComplete(backupID)).WillReturnError(fmt.Errorf("DbError"))
		case "1stGetTruncateDbError":
			var backupID string = "1038347234"
			rows1 := mock.NewRows([]string{"BACKUP_ID"}).AddRow(backupID)
			mock.ExpectQuery(q_GetLatestFullBackupID(uint(tt.args.days))).WillReturnRows(rows1)
			mock.ExpectQuery(f_GetTruncateData(backupID)).WillReturnError(fmt.Errorf("DbError"))
		case "1stGetTruncateScanError":
			var backupID string = "1038347234"
			rows1 := mock.NewRows([]string{"BACKUP_ID"}).AddRow(backupID)
			rows2 := mock.NewRows([]string{"FILES", "BACKUP_SIZE"})
			rows2.AddRow("100", "1024000.12")
			mock.ExpectQuery(q_GetLatestFullBackupID(uint(tt.args.days))).WillReturnRows(rows1)
			mock.ExpectQuery(f_GetTruncateData(backupID)).WillReturnRows(rows2)
		case "GetBackupIdScanError":
			var backupID string = "34534.45"
			rows1 := mock.NewRows([]string{"BACKUP_ID"}).AddRow(backupID)
			mock.ExpectQuery(q_GetLatestFullBackupID(uint(tt.args.days))).WillReturnRows(rows1)
		case "GetBackupIdDbError":
			mock.ExpectQuery(q_GetLatestFullBackupID(uint(tt.args.days))).WillReturnError(fmt.Errorf("DbError"))
		default:
			fmt.Printf("No test case matched for %s\n", tt.name)
			t.Errorf("No test case matched")
		}
		t.Run(tt.name, func(t *testing.T) {
			h := &hanaUtilClient{
				db:  tt.fields.db,
				dsn: tt.fields.dsn,
			}
			got, err := h.TruncateBackupCatalog(tt.args.days, tt.args.complete)
			if (err != nil) != tt.wantErr {
				t.Errorf("hanaUtilClient.TruncateBackupCatalog() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("hanaUtilClient.TruncateBackupCatalog() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_hanaUtilClient_RemoveTraceFile(t *testing.T) {
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
		host     string
		filename string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"Good", fields{db1, ""}, args{"host1", "traceFile.trc"}, false},
		{"TraceNotRemoved", fields{db1, ""}, args{"sap-hana-prod01", "nameserver-sap-hana-prod01-0000.00012.trc"}, true},
		{"2ndGetTraceDbError", fields{db1, ""}, args{"sap-hana-prod01", "nameserver-sap-hana-prod01-0000.00012.trc.gz"}, true},
		{"2ndGetTraceScanError", fields{db1, ""}, args{"sap-hana-prod01", "nameserver-sap-hana-prod01-0000.00012.trc.gz"}, true},
		{"RemoveTraceDbError", fields{db1, ""}, args{"srv1", "xsengine-srv1-00069.trc."}, true},
		{"TraceNotFound", fields{db1, ""}, args{"m-y-b-o-x", "nameserver-m-y-b-o-x-001.trc."}, true},
		/*Below is a real edge case, I don't think it could ever happen*/
		{"TraceNotUnique", fields{db1, ""}, args{"m-y-b-o-x", "nameserver-m-y-b-o-x-001.trc."}, true},
		{"1stGetTraceDbError", fields{db1, ""}, args{"host1", "traceFile.trc"}, true},
		{"1stGetTraceScanError", fields{db1, ""}, args{"host1", "traceFile.trc"}, true},
	}
	for _, tt := range tests {
		/*Per case mocking*/
		switch tt.name {
		case "Good":
			row1 := mock.NewRows([]string{"COUNT"}).AddRow("1")
			row2 := mock.NewRows([]string{"COUNT"}).AddRow("0")
			mock.ExpectQuery(f_GetTraceFile(tt.args.host, tt.args.filename)).WillReturnRows(row1)
			mock.ExpectExec(f_RemoveTraceFile(tt.args.host, tt.args.filename)).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(f_GetTraceFile(tt.args.host, tt.args.filename)).WillReturnRows(row2)
		case "TraceNotRemoved":
			row1 := mock.NewRows([]string{"COUNT"}).AddRow("1")
			row2 := mock.NewRows([]string{"COUNT"}).AddRow("1")
			mock.ExpectQuery(f_GetTraceFile(tt.args.host, tt.args.filename)).WillReturnRows(row1)
			mock.ExpectExec(f_RemoveTraceFile(tt.args.host, tt.args.filename)).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(f_GetTraceFile(tt.args.host, tt.args.filename)).WillReturnRows(row2)
		case "2ndGetTraceDbError":
			row1 := mock.NewRows([]string{"COUNT"}).AddRow("1")
			mock.ExpectQuery(f_GetTraceFile(tt.args.host, tt.args.filename)).WillReturnRows(row1)
			mock.ExpectExec(f_RemoveTraceFile(tt.args.host, tt.args.filename)).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(f_GetTraceFile(tt.args.host, tt.args.filename)).WillReturnError(fmt.Errorf("DbError"))
		case "2ndGetTraceScanError":
			row1 := mock.NewRows([]string{"COUNT"}).AddRow("1")
			row2 := mock.NewRows([]string{"COUNT"}).AddRow("1.5")
			mock.ExpectQuery(f_GetTraceFile(tt.args.host, tt.args.filename)).WillReturnRows(row1)
			mock.ExpectExec(f_RemoveTraceFile(tt.args.host, tt.args.filename)).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(f_GetTraceFile(tt.args.host, tt.args.filename)).WillReturnRows(row2)
		case "RemoveTraceDbError":
			row1 := mock.NewRows([]string{"COUNT"}).AddRow("1")
			mock.ExpectQuery(f_GetTraceFile(tt.args.host, tt.args.filename)).WillReturnRows(row1)
			mock.ExpectExec(f_RemoveTraceFile(tt.args.host, tt.args.filename)).WillReturnError(fmt.Errorf("DbError"))
		case "TraceNotFound":
			row1 := mock.NewRows([]string{"COUNT"}).AddRow("0")
			mock.ExpectQuery(f_GetTraceFile(tt.args.host, tt.args.filename)).WillReturnRows(row1)
		case "TraceNotUnique":
			row1 := mock.NewRows([]string{"COUNT"}).AddRow("2")
			mock.ExpectQuery(f_GetTraceFile(tt.args.host, tt.args.filename)).WillReturnRows(row1)
		case "1stGetTraceDbError":
			mock.ExpectQuery(f_GetTraceFile(tt.args.host, tt.args.filename)).WillReturnError(fmt.Errorf("DbError"))
		case "1stGetTraceScanError":
			row1 := mock.NewRows([]string{"COUNT"}).AddRow("1.5")
			mock.ExpectQuery(f_GetTraceFile(tt.args.host, tt.args.filename)).WillReturnRows(row1)
		default:
			fmt.Printf("No test case matched for %s\n", tt.name)
			t.Errorf("No test case matched")
		}
		t.Run(tt.name, func(t *testing.T) {
			h := &hanaUtilClient{
				db:  tt.fields.db,
				dsn: tt.fields.dsn,
			}
			if err := h.RemoveTraceFile(tt.args.host, tt.args.filename); (err != nil) != tt.wantErr {
				t.Errorf("hanaUtilClient.RemoveTraceFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_hanaUtilClient_RemoveStatServerAlerts(t *testing.T) {
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
		want    uint64
		wantErr bool
	}{
		{"GoodAllDeleted", fields{db1, ""}, args{42}, 99, false},
		{"GoodSomeDeleted", fields{db1, ""}, args{99}, 30, false},
		{"NothingDeleted", fields{db1, ""}, args{0}, 0, false},
		{"NothingDeletedButMoreFound", fields{db1, ""}, args{10}, 0, false},
		{"2ndGetStatServerAlertsDbError", fields{db1, ""}, args{28}, 0, true},
		{"2ndGetStatServerAlertsScanError", fields{db1, ""}, args{28}, 0, true},
		{"RemoveStatServerAlertsDbError", fields{db1, ""}, args{99}, 0, true},
		{"1stGetStatServerAlertsDbError", fields{db1, ""}, args{30}, 0, true},
		{"1stGetStatServerAlertsScanError", fields{db1, ""}, args{7}, 0, true},
	}
	for _, tt := range tests {
		/*Set up per case mocking*/
		switch tt.name {
		case "GoodAllDeleted":
			rows1 := sqlmock.NewRows([]string{"COUNT"}).AddRow(99)
			rows2 := sqlmock.NewRows([]string{"COUNT"}).AddRow(0)
			mock.ExpectQuery(f_GetStatServerAlerts(tt.args.days)).WillReturnRows(rows1)
			mock.ExpectExec(f_RemoveStatServerAlerts(tt.args.days)).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(f_GetStatServerAlerts(tt.args.days)).WillReturnRows(rows2)
		case "GoodSomeDeleted":
			rows1 := sqlmock.NewRows([]string{"COUNT"}).AddRow(35)
			rows2 := sqlmock.NewRows([]string{"COUNT"}).AddRow(5)
			mock.ExpectQuery(f_GetStatServerAlerts(tt.args.days)).WillReturnRows(rows1)
			mock.ExpectExec(f_RemoveStatServerAlerts(tt.args.days)).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(f_GetStatServerAlerts(tt.args.days)).WillReturnRows(rows2)
		case "NothingDeleted":
			rows1 := sqlmock.NewRows([]string{"COUNT"}).AddRow(2)
			rows2 := sqlmock.NewRows([]string{"COUNT"}).AddRow(2)
			mock.ExpectQuery(f_GetStatServerAlerts(tt.args.days)).WillReturnRows(rows1)
			mock.ExpectExec(f_RemoveStatServerAlerts(tt.args.days)).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(f_GetStatServerAlerts(tt.args.days)).WillReturnRows(rows2)
		case "NothingDeletedButMoreFound":
			rows1 := sqlmock.NewRows([]string{"COUNT"}).AddRow(16)
			rows2 := sqlmock.NewRows([]string{"COUNT"}).AddRow(17)
			mock.ExpectQuery(f_GetStatServerAlerts(tt.args.days)).WillReturnRows(rows1)
			mock.ExpectExec(f_RemoveStatServerAlerts(tt.args.days)).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(f_GetStatServerAlerts(tt.args.days)).WillReturnRows(rows2)
		case "2ndGetStatServerAlertsDbError":
			rows1 := sqlmock.NewRows([]string{"COUNT"}).AddRow(16)
			mock.ExpectQuery(f_GetStatServerAlerts(tt.args.days)).WillReturnRows(rows1)
			mock.ExpectExec(f_RemoveStatServerAlerts(tt.args.days)).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(f_GetStatServerAlerts(tt.args.days)).WillReturnError(fmt.Errorf("DbError"))
		case "2ndGetStatServerAlertsScanError":
			rows1 := sqlmock.NewRows([]string{"COUNT"}).AddRow(16)
			rows2 := sqlmock.NewRows([]string{"COUNT"}).AddRow("17.5")
			mock.ExpectQuery(f_GetStatServerAlerts(tt.args.days)).WillReturnRows(rows1)
			mock.ExpectExec(f_RemoveStatServerAlerts(tt.args.days)).WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(f_GetStatServerAlerts(tt.args.days)).WillReturnRows(rows2)
		case "RemoveStatServerAlertsDbError":
			rows1 := sqlmock.NewRows([]string{"COUNT"}).AddRow(16)
			mock.ExpectQuery(f_GetStatServerAlerts(tt.args.days)).WillReturnRows(rows1)
			mock.ExpectExec(f_RemoveStatServerAlerts(tt.args.days)).WillReturnError(fmt.Errorf("DbError"))
		case "1stGetStatServerAlertsDbError":
			mock.ExpectQuery(f_GetStatServerAlerts(tt.args.days)).WillReturnError(fmt.Errorf("DbError"))
		case "RemoveStatServerAlertsScanError":
			rows1 := sqlmock.NewRows([]string{"COUNT"}).AddRow("1.111")
			mock.ExpectQuery(f_GetStatServerAlerts(tt.args.days)).WillReturnRows(rows1)
		default:
			fmt.Printf("No test case matched for %s\n", tt.name)
			t.Errorf("No test case matched")
		}
		t.Run(tt.name, func(t *testing.T) {
			h := &hanaUtilClient{
				db:  tt.fields.db,
				dsn: tt.fields.dsn,
			}
			got, err := h.RemoveStatServerAlerts(tt.args.days)
			if (err != nil) != tt.wantErr {
				t.Errorf("hanaUtilClient.RemoveStatServerAlerts() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("hanaUtilClient.RemoveStatServerAlerts() = %v, want %v", got, tt.want)
			}
		})
	}
}
