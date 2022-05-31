package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/jmoiron/sqlx"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// CoreEvent ...
type CoreEvent struct {
	ID        uint64
	Data      []byte
	CreatedAt time.Time
}

// BinlogCommittedEvent ...
type BinlogCommittedEvent struct {
	Events  []CoreEvent
	GTIDSet string
}

const flavorMysql = "mysql"

func readFromBinlog(syncer *replication.BinlogSyncer, lastGTIDSet string, output chan<- BinlogCommittedEvent) {
	start, err := mysql.ParseGTIDSet(flavorMysql, lastGTIDSet)
	if err != nil {
		panic(err)
	}

	stream, err := syncer.StartSyncGTID(start)
	if err != nil {
		panic(err)
	}

	events := make([]CoreEvent, 0, 1024)

	ctx := context.Background()
	for {
		event, err := stream.GetEvent(ctx)
		if err != nil {
			panic(err)
		}

		rowEvent, ok := event.Event.(*replication.RowsEvent)
		if ok {
			if string(rowEvent.Table.Table) == "core_event" {
				for _, row := range rowEvent.Rows {
					events = append(events, CoreEvent{
						ID:        uint64(row[0].(int64)),
						Data:      row[1].([]byte),
						CreatedAt: row[2].(time.Time),
					})
				}
			}
			continue
		}

		xidEvent, ok := event.Event.(*replication.XIDEvent)
		if ok {
			if len(events) > 0 {
				copiedEvents := make([]CoreEvent, len(events))
				copy(copiedEvents, events)
				events = events[:0]

				output <- BinlogCommittedEvent{
					Events:  copiedEvents,
					GTIDSet: xidEvent.GSet.String(),
				}
			} else {
				output <- BinlogCommittedEvent{
					GTIDSet: xidEvent.GSet.String(),
				}
			}

			continue
		}
	}
}

func main() {
	db := sqlx.MustConnect(flavorMysql, "root:1@tcp(localhost:3306)/bench?parseTime=true")

	var lastGTIDSet string
	err := db.Get(&lastGTIDSet, "SELECT gtid_set FROM last_event_seq WHERE name = ?", "core_event")
	if err != nil && err == sql.ErrNoRows {
		fmt.Println(err)
	}

	fmt.Println("LAST GTID SET:", lastGTIDSet)

	conf := replication.BinlogSyncerConfig{
		ServerID:  101,
		Flavor:    flavorMysql,
		Host:      "localhost",
		Port:      3306,
		User:      "root",
		Password:  "1",
		ParseTime: true,
	}

	syncer := replication.NewBinlogSyncer(conf)

	ch := make(chan BinlogCommittedEvent, 1024)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		readFromBinlog(syncer, lastGTIDSet, ch)
	}()

	go func() {
		defer wg.Done()

		for e := range ch {
			for _, event := range e.Events {
				fmt.Println(event.ID, string(event.Data))
			}
			if len(e.Events) > 0 {
				db.MustExec(`
INSERT INTO last_event_seq (name, gtid_set)
VALUES (?, ?) AS NEW
ON DUPLICATE KEY UPDATE gtid_set = NEW.gtid_set
`, "core_event", e.GTIDSet)
			}
		}
	}()

	wg.Wait()
}
