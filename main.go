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
	From int64
	To   int64
}

// GTIDSet ...
type GTIDSet map[string][]Interval

// GTID ...
type GTID struct {
	SID []byte
	GNO int64
}

func intervalLowerBound(intervals []Interval, n int64) (result int, existed bool) {
	result = -1
	for i, interval := range intervals {
		if interval.To > n {
			return result, interval.From <= n
		}
		result = i
	}
	return result, false
}

func intervalInsertAt(intervals []Interval, index int, val Interval) []Interval {
	n := len(intervals)
	intervals = append(intervals, Interval{})
	for i := n; i > index; i-- {
		intervals[i] = intervals[i-1]
	}
	intervals[index] = val
	return intervals
}

func intervalRemoveAt(intervals []Interval, index int) []Interval {
	n := len(intervals)
	for i := index + 1; i < n; i++ {
		intervals[i-1] = intervals[i]
	}
	intervals = intervals[:n-1]
	return intervals
}

func intervalJoinToNext(intervals []Interval, index int) ([]Interval, bool) {
	prevTo := intervals[index].To
	nextIndex := index + 1
	if len(intervals) > nextIndex && intervals[nextIndex].From == prevTo+1 {
		newTo := intervals[nextIndex].To
		intervals[index].To = newTo
		intervals = intervalRemoveAt(intervals, nextIndex)
		return intervals, true
	}
	return intervals, false
}

// Add ...
func (s GTIDSet) Add(id GTID) {
	idStr := string(id.SID)
	intervals := s[idStr]

	index, existed := intervalLowerBound(intervals, id.GNO-1)
	if existed {
		return
	}

	if index < 0 || id.GNO > intervals[index].To+1 {
		index = index + 1
		intervals = intervalInsertAt(intervals, index, Interval{
			From: id.GNO,
			To:   id.GNO,
		})
		s[idStr] = intervals
	} else {
		intervals[index].To = id.GNO
	}

	var ok bool
	for {
		intervals, ok = intervalJoinToNext(intervals, index)
		if !ok {
			break
		}
		s[idStr] = intervals
	}
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
