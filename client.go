package hanautil

/**
This file defines the HanaUtilClient the methods of returning
a new client.
**/

import (
	"database/sql"

	_ "github.com/SAP/go-hdb/driver"
)

type hanaUtilClient struct {
	db  *sql.DB //non-exported database connection
	dsn string  //non-exported dsn, used to create connection
}

func NewClient(dsn string) *hanaUtilClient {
	/*should we do some basic dsn format testing?*/
	return &hanaUtilClient{nil, dsn}
}

func (h *hanaUtilClient) Connect() error {
	var err error
	h.db, err = sql.Open("hdb", h.dsn)
	if err != nil {
		return err
	}

	err = h.db.Ping()
	if err != nil {
		return err
	}

	return nil
}

func (h *hanaUtilClient) Close() error {
	return h.db.Close()
}
