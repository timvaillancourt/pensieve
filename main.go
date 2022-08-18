package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

type Binlog struct {
	LogName  string
	LogPath  string
	FileSize int64
}

type ServerConfig struct {
	GtidMode       string
	LogBin         bool
	LogBinBasename string
	LogBinIndex    string
}

type BinlogFill struct {
	db               *sql.DB
	origGtidExecuted *gomysql.UUIDSet
	serverCfg        ServerConfig
}

func (bf *BinlogFill) binlogIndexEntry(binlog Binlog) string {
	dir := filepath.Dir(bf.serverCfg.LogBinBasename)
	if filepath.Dir(bf.serverCfg.LogBinBasename) == filepath.Dir(bf.serverCfg.LogBinIndex) {
		dir = "."
	}
	return fmt.Sprintf("%s/%s", dir, binlog.LogName)
}

func (bf *BinlogFill) writeBinlogIndex(binlogs []Binlog) error {
	sort.Slice(binlogs, func(i, j int) bool {
		return binlogs[i].LogName < binlogs[j].LogName
	})

	f, err := os.Open(bf.serverCfg.LogBinIndex)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, binlog := range binlogs {
		if _, err = fmt.Fprintln(f, bf.binlogIndexEntry(binlog)); err != nil {
			return nil
		}
	}
	return nil
}

func (bf *BinlogFill) getBinaryLogs() (binlogs []Binlog, err error) {
	query := `SHOW BINARY LOGS`
	rows, err := bf.db.Query(query)
	if err != nil {
		log.Fatalf("Failed to run %q query: %+v", query, err)
		return nil, err
	}
	defer rows.Close()

	var binlog Binlog
	for rows.Next() {
		if err = rows.Scan(&binlog.LogName, &binlog.FileSize); err != nil {
			return nil, err
		}
		binlogs = append(binlogs, binlog)
	}
	return binlogs, err
}

type MasterStatus struct {
	ExecGtidSet *gomysql.UUIDSet
}

func (bf *BinlogFill) getMasterStatus() (MasterStatus, error) {
	query := `SHOW MASTER STATUS`
	if err := bf.db.QueryRow(query).Scan(&srcUUID, &interval.Start, &interval.Stop); err != nil {
		log.Fatalf("Failed to run %q query: %+v", query, err)
		return nil, err
	}

	sid, err := uuid.Parse(srcUUID)
	if err != nil {
		return nil, err
	}
	return &gomysql.UUIDSet{
		SID:       sid,
		Intervals: gomysql.IntervalSlice{interval},
	}, nil
}

func (bf *BinlogFill) readServerConfig() (serverCfg ServerConfig, err error) {
	query := `select @@global.log_bin, @@global.log_bin_basename, @@global.log_bin_index, @@global.gtid_mode`
	if err := bf.db.QueryRow(query).Scan(
		&serverCfg.LogBin,
		&serverCfg.LogBinBasename,
		&serverCfg.LogBinIndex,
		&serverCfg.GtidMode,
	); err != nil {
		log.Fatalf("Failed to run %q query: %+v", query, err)
		return err
	}
	return serverCfg, err
}

/*
func (bf *BinlogFill) loadServerConfig() (serverCfg ServerConfig, err error) {
	serverCfg, err := bf.readServerConfig()
	if err != nil {
		return serverCfg, err
	}

	if !serverCfg.LogBin {
		return serverCfg, errLogBinDisabled
	}
*/

func (bf *BinlogFill) resetReplication() (err error) {
	queries := []string{
		`STOP SLAVE`,
		`RESET SLAVE`,
		`RESET MASTER`,
	}
	for _, query := range queries {
		if _, err = bf.db.Exec(query); err != nil {
			log.Printf("Failed to run %q query: %+v", query, err)
			return err
		}
	}
	log.Println("Reset replication states")
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

	err = bf.readServerConfig()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("readServerConfig: %+v", bf.serverCfg)

	binaryLogs, err := bf.getBinaryLogs()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("getBinaryLogs: %+v", binaryLogs)

	if bf.origGtidExecuted, err = bf.getGtidExecuted(); err != nil {
		log.Fatal(err)
	}
	if bf.origGtidExecuted == nil {
		log.Println("readGtidExecuted: returned no gtid state, assuming host ran RESET SLAVE")
	} else {
		log.Printf("readGtidExecuted: %+v", bf.origGtidExecuted)
	}

	/*
		if err = bf.resetReplication(); err != nil {
			log.Fatal(err)
		}
	*/

	gtidExecuted, err := bf.getGtidExecuted()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("final readGtidExecuted: %+v", gtidExecuted)
}
