package main

import (
	"flag"
	"fmt"
	"log"
	"os/user"
	"strconv"
	"strings"
	"time"

	"gopkg.in/ini.v1"

	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

type strValueScanner struct {
	valid bool
	value interface{}
}

func (scanner *strValueScanner) getBytes(src interface{}) []byte {
	if a, ok := src.([]uint8); ok {
		return a
	}
	return nil
}

func (scanner *strValueScanner) Scan(src interface{}) error {
	switch v := src.(type) {
	case int64:
		scanner.value = strconv.FormatInt(v, 10)
		scanner.valid = true
	case float64:
		scanner.value = strconv.FormatFloat(v, 'f', -1, 64)
		scanner.valid = true
	case bool:
		scanner.value = strconv.FormatBool(v)
		scanner.valid = true
	case string:
		scanner.value = v
		scanner.valid = true
	case []byte:
		scanner.value = fmt.Sprintf("%s", scanner.getBytes(src))
		scanner.valid = true
	case nil:
		scanner.value = "null"
		scanner.valid = true
	}
	return nil
}

type valuesArray []string

func (i *valuesArray) String() string {
	return fmt.Sprintf("%d", i)
}

func (i *valuesArray) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func check(source string, query string, values []string, field string) bool {
	db, err := sql.Open("mysql", source)
	defer db.Close()
	failOnError(err, "Failed to connect to MySQL")
	stmt, err := db.Prepare(query)
	defer stmt.Close()
	failOnError(err, "Failed to prepare statement")

	rows, _ := stmt.Query()
	columns, _ := rows.Columns()
	dbValue := make([]interface{}, len(columns))

	for rows.Next() {
		for i := range columns {
			dbValue[i] = new(strValueScanner)
		}

		err := rows.Scan(dbValue...)
		failOnError(err, "Failed to read values")

		for i, column := range columns {
			var f = dbValue[i].(*strValueScanner)
			if field != "" && column != field {
				continue
			}

			for _, value := range values {
				if value == f.value {
					log.Printf("field: %s, got value: {%s}", column, f.value)
					return true
				}
			}
		}
	}
	return false
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func patchConnectionString(source string, credential string) string {

	if credential != "" {
		myCnf, err := ini.Load(credential)
		failOnError(err, "Failed to read credentials from file")

		username := myCnf.Section("client").Key("user").String()
		if username == "" {
			systemUser, err := user.Current()
			failOnError(err, "Get current user failed")
			username = systemUser.Username
		}

		parts := strings.Split(source, "@")
		return fmt.Sprintf("%s:%s@%s",
			username,
			myCnf.Section("client").Key("password").String(),
			parts[len(parts)-1])
	}
	return source
}

func main() {

	var source, query, field, credential string
	var values valuesArray

	flag.StringVar(&source, "source", "tcp(127.0.0.1:3306)/", "MySQL connection string format 'user:password@tcp(localhost:port)/database'")
	flag.StringVar(&credential, "credential", "", "Path to MySQL client credentials cnf/ini file")
	flag.StringVar(&query, "query", "select 1", "query")
	flag.Var(&values, "value", "value to wait for (-value 1 -value 2 to wait any of values)")
	flag.StringVar(&field, "field", "", "field name to look at, emtpy value = any field")

	flag.Parse()

	connectionString := strings.TrimSpace(patchConnectionString(source, credential))

	if len(values) == 0 {
		flag.PrintDefaults()
		log.Fatal("Need at least one value")
	}

	log.Printf("Waiting for values: {%s} (at field '%s')", strings.Join(values, "}|{"), field)
	for check(connectionString, query, values, field) == false {
		time.Sleep(time.Second * 1)
	}

}
