package main

import (
	"context"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"time"
)

// Event ...
type Event struct {
	ID        uint64
	Data      []byte
	CreatedAt time.Time
}

// Interval ...
type Interval struct {
	Start int64
	End   int64
}

// GTIDSet ...
type GTIDSet map[string][]Interval

// GTID ...
type GTID struct {
	SID []byte
	GNO int64
}

// Add ...
func (s GTIDSet) Add(id GTID) {
}

func main() {
	const flavorMysql = "mysql"
	conf := replication.BinlogSyncerConfig{
		ServerID: 101,
		Flavor:   flavorMysql,
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "1",
	}

	syncer := replication.NewBinlogSyncer(conf)

	start, err := mysql.ParseGTIDSet(flavorMysql, "")
	if err != nil {
		panic(err)
	}

	stream, err := syncer.StartSyncGTID(start)
	if err != nil {
		panic(err)
	}

	var lastGTID GTID
	events := make([]Event, 0, 1024)
	set := GTIDSet{}

	ctx := context.Background()
	for {
		event, err := stream.GetEvent(ctx)
		if err != nil {
			panic(err)
		}

		gtidEvent, ok := event.Event.(*replication.GTIDEvent)
		if ok {
			lastGTID = GTID{
				SID: gtidEvent.SID,
				GNO: gtidEvent.GNO,
			}
			continue
		}

		rowEvent, ok := event.Event.(*replication.RowsEvent)
		if ok {
			if string(rowEvent.Table.Table) == "core_event" {
				for _, row := range rowEvent.Rows {
					const layout = "2006-01-02 15:04:05"
					createdAt, err := time.Parse(layout, row[2].(string))
					if err != nil {
						panic(err)
					}

					events = append(events, Event{
						ID:        uint64(row[0].(int64)),
						Data:      row[1].([]byte),
						CreatedAt: createdAt,
					})
				}
			}
			continue
		}

		xidEvent, ok := event.Event.(*replication.XIDEvent)
		if ok {
			fmt.Println("XID:", xidEvent.XID, xidEvent.GSet)
			for _, e := range events {
				fmt.Println(e.ID)
				fmt.Println(string(e.Data))
			}
			events = events[:0]
			fmt.Println(lastGTID)
			set.Add(lastGTID)
			continue
		}
	}
}
