package main

import (
	"github.com/google/uuid"
	"sort"
	"strconv"
	"strings"
)

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

// String ...
func (s GTIDSet) String() string {
	array := make([]string, 0, len(s))
	for key := range s {
		array = append(array, key)
	}
	sort.Strings(array)

	var buf strings.Builder
	for i, key := range array {
		if i > 0 {
			buf.WriteString(",")
		}

		list := s[key]

		var id uuid.UUID
		copy(id[:], key)

		_, _ = buf.WriteString(id.String())

		for _, interval := range list {
			_, _ = buf.WriteString(":")
			_, _ = buf.WriteString(strconv.FormatInt(interval.From, 10))
			_, _ = buf.WriteString("-")
			_, _ = buf.WriteString(strconv.FormatInt(interval.To, 10))
		}
	}

	return buf.String()
}
