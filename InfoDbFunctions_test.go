package hanautil

import (
	"database/sql"
	"fmt"
	"testing"

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
