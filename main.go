package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

type Binlog struct {
	LogName  string
	FileSize int64
}

type LogBinConfig struct {
	Basename string
	Index    string
}

type BinlogFill struct {
	db           *sql.DB
	gtidExecuted *gomysql.UUIDSet
	logBinCfg    LogBinConfig
}

func (bf *BinlogFill) readBinaryLogs() (binlogs []Binlog, err error) {
	query := `SHOW BINARY LOGS`
	rows, err := bf.db.Query(query)
	if err != nil {
		log.Fatalf("Failed to run %q query: %+v", query, err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var binlog Binlog
		if err = rows.Scan(&binlog.LogName, &binlog.FileSize); err != nil {
			return nil, err
		}
		binlogs = append(binlogs, binlog)
	}
	return binlogs, err
}

func (bf *BinlogFill) readGtidExecuted() error {
	var srcUUID string
	var interval gomysql.Interval
	query := `SELECT source_uuid, interval_start, interval_end FROM mysql.gtid_executed`
	if err := bf.db.QueryRow(query).Scan(&srcUUID, &interval.Start, &interval.Stop); err != nil {
		log.Fatalf("Failed to run %q query: %+v", query, err)
		return err
	}

	sid, err := uuid.Parse(srcUUID)
	if err != nil {
		return err
	}

	bf.gtidExecuted = &gomysql.UUIDSet{
		SID:       sid,
		Intervals: gomysql.IntervalSlice{interval},
	}
	return err
}

func (bf *BinlogFill) readLogBinConfig() error {
	var logBinCfg LogBinConfig
	query := `select @@global.log_bin_basename, @@global.log_bin_index`
	if err := bf.db.QueryRow(query).Scan(&logBinCfg.Basename, &logBinCfg.Index); err != nil {
		log.Fatalf("Failed to run %q query: %+v", query, err)
		return err
	}
	bf.logBinCfg = logBinCfg
	return nil
}

func main() {
	var host, user, password string
	var port int64

	flag.StringVar(&host, "host", "127.0.0.1", "mysql host")
	flag.StringVar(&user, "user", "root", "mysql user name")
	flag.StringVar(&password, "password", "", "mysql user password")
	flag.Int64Var(&port, "port", 3306, "mysql port")
	flag.Parse()

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/information_schema?interpolateParams=true",
		user, password, host, port,
	))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	bf := &BinlogFill{db: db}

	err = bf.readLogBinConfig()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("readLogBinConfig: %+v", bf.logBinCfg)

	binaryLogs, err := bf.readBinaryLogs()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("readBinaryLogs: %+v", binaryLogs)

	if err = bf.readGtidExecuted(); err != nil {
		log.Fatal(err)
	}
	log.Printf("readGtidExecuted: %+v", bf.gtidExecuted)
}
