package hanautil

import (
	"testing"
)

func Test_f_GetTraceFiles(t *testing.T) {
	type args struct {
		days uint
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"Good0", args{0}, "SELECT HOST, FILE_NAME, FILE_SIZE, FILE_MTIME FROM \"SYS\".\"M_TRACEFILES\" WHERE FILE_MTIME < (SELECT ADD_DAYS(NOW(), -0) FROM DUMMY) AND RIGHT(FILE_NAME, 3) = 'trc' OR FILE_MTIME < (SELECT ADD_DAYS(NOW(), -0) FROM DUMMY) AND RIGHT(FILE_NAME, 2) = 'gz'"},
		{"Good10", args{10}, "SELECT HOST, FILE_NAME, FILE_SIZE, FILE_MTIME FROM \"SYS\".\"M_TRACEFILES\" WHERE FILE_MTIME < (SELECT ADD_DAYS(NOW(), -10) FROM DUMMY) AND RIGHT(FILE_NAME, 3) = 'trc' OR FILE_MTIME < (SELECT ADD_DAYS(NOW(), -10) FROM DUMMY) AND RIGHT(FILE_NAME, 2) = 'gz'"},
		{"Good14", args{14}, "SELECT HOST, FILE_NAME, FILE_SIZE, FILE_MTIME FROM \"SYS\".\"M_TRACEFILES\" WHERE FILE_MTIME < (SELECT ADD_DAYS(NOW(), -14) FROM DUMMY) AND RIGHT(FILE_NAME, 3) = 'trc' OR FILE_MTIME < (SELECT ADD_DAYS(NOW(), -14) FROM DUMMY) AND RIGHT(FILE_NAME, 2) = 'gz'"},
		{"Good21", args{21}, "SELECT HOST, FILE_NAME, FILE_SIZE, FILE_MTIME FROM \"SYS\".\"M_TRACEFILES\" WHERE FILE_MTIME < (SELECT ADD_DAYS(NOW(), -21) FROM DUMMY) AND RIGHT(FILE_NAME, 3) = 'trc' OR FILE_MTIME < (SELECT ADD_DAYS(NOW(), -21) FROM DUMMY) AND RIGHT(FILE_NAME, 2) = 'gz'"},
		{"Good30", args{30}, "SELECT HOST, FILE_NAME, FILE_SIZE, FILE_MTIME FROM \"SYS\".\"M_TRACEFILES\" WHERE FILE_MTIME < (SELECT ADD_DAYS(NOW(), -30) FROM DUMMY) AND RIGHT(FILE_NAME, 3) = 'trc' OR FILE_MTIME < (SELECT ADD_DAYS(NOW(), -30) FROM DUMMY) AND RIGHT(FILE_NAME, 2) = 'gz'"},
		{"Good90", args{90}, "SELECT HOST, FILE_NAME, FILE_SIZE, FILE_MTIME FROM \"SYS\".\"M_TRACEFILES\" WHERE FILE_MTIME < (SELECT ADD_DAYS(NOW(), -90) FROM DUMMY) AND RIGHT(FILE_NAME, 3) = 'trc' OR FILE_MTIME < (SELECT ADD_DAYS(NOW(), -90) FROM DUMMY) AND RIGHT(FILE_NAME, 2) = 'gz'"},
		{"Good100", args{100}, "SELECT HOST, FILE_NAME, FILE_SIZE, FILE_MTIME FROM \"SYS\".\"M_TRACEFILES\" WHERE FILE_MTIME < (SELECT ADD_DAYS(NOW(), -100) FROM DUMMY) AND RIGHT(FILE_NAME, 3) = 'trc' OR FILE_MTIME < (SELECT ADD_DAYS(NOW(), -100) FROM DUMMY) AND RIGHT(FILE_NAME, 2) = 'gz'"},
		{"Good365", args{365}, "SELECT HOST, FILE_NAME, FILE_SIZE, FILE_MTIME FROM \"SYS\".\"M_TRACEFILES\" WHERE FILE_MTIME < (SELECT ADD_DAYS(NOW(), -365) FROM DUMMY) AND RIGHT(FILE_NAME, 3) = 'trc' OR FILE_MTIME < (SELECT ADD_DAYS(NOW(), -365) FROM DUMMY) AND RIGHT(FILE_NAME, 2) = 'gz'"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := f_GetTraceFiles(tt.args.days); got != tt.want {
				t.Errorf("f_GetTraceFiles() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_q_DefragDataVolAll(t *testing.T) {
	type args struct {
		pct uint
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{"Good01", args{120}, "ALTER SYSTEM RECLAIM DATAVOLUME 120 DEFRAGMENT", false},
		{"Good02", args{105}, "ALTER SYSTEM RECLAIM DATAVOLUME 105 DEFRAGMENT", false},
		{"Good03", args{150}, "ALTER SYSTEM RECLAIM DATAVOLUME 150 DEFRAGMENT", false},
		{"DefaultDefragPct", args{0}, "ALTER SYSTEM RECLAIM DATAVOLUME 120 DEFRAGMENT", false},
		{"TooLow", args{90}, "", true},
		{"TooHigh", args{200}, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := q_DefragDataVolAll(tt.args.pct)
			if (err != nil) != tt.wantErr {
				t.Errorf("q_DefragDataVolAll() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("q_DefragDataVolAll() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_q_DefragDataVol(t *testing.T) {
	type args struct {
		host string
		port uint
		pct  uint
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{"Good01", args{"hdb01", 30013, 120}, "ALTER SYSTEM RECLAIM DATAVOLUME 'hdb01:30013' 120 DEFRAGMENT", false},
		{"Good02", args{"localhost", 30015, 107}, "ALTER SYSTEM RECLAIM DATAVOLUME 'localhost:30015' 107 DEFRAGMENT", false},
		{"Good03", args{"localhost", 34040, 115}, "ALTER SYSTEM RECLAIM DATAVOLUME 'localhost:34040' 115 DEFRAGMENT", false},
		{"DefaultDefragPct", args{"localhost", 34040, 0}, "ALTER SYSTEM RECLAIM DATAVOLUME 'localhost:34040' 120 DEFRAGMENT", false},
		{"TooHigh", args{"hdb01", 30013, 175}, "", true},
		{"TooLow", args{"hdb01", 30013, 104}, "", true},
		{"NoHost", args{"", 30013, 120}, "", true},
		{"NoPort", args{"hdb01", 0, 120}, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := q_DefragDataVol(tt.args.host, tt.args.port, tt.args.pct)
			if (err != nil) != tt.wantErr {
				t.Errorf("q_DefragDataVol() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("q_DefragDataVol() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_q_GetDataVolume(t *testing.T) {
	type args struct {
		host string
		port uint
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"Good01", args{"demo", 30003}, "SELECT \"SYS\".\"HOST\", \"SYS\".\"PORT\", \"SRV\".\"SERVICE_NAME\", \"SYS\".\"TOTAL_SIZE\", \"SYS\".\"USED_SIZE\" FROM \"SYS\".\"M_VOLUME_FILES\" AS SYS LEFT JOIN \"SYS\".\"M_SERVICES\" AS SRV ON \"SYS\".\"PORT\" = \"SRV\".\"PORT\" WHERE \"SYS\".\"FILE_TYPE\" = 'DATA' AND \"SYS\".\"HOST\" = 'demo' AND \"SYS\".\"PORT\" = '30003';"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := q_GetDataVolume(tt.args.host, tt.args.port); got != tt.want {
				t.Errorf("q_GetDataVolume() = %v, want %v", got, tt.want)
			}
		})
	}
}
