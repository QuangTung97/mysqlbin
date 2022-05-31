package main

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

func TestGTIDSet__Single_SID(t *testing.T) {
	s := GTIDSet{}
	id := uuid.New()
	idStr := string(id[:])

	s.Add(GTID{SID: id[:], GNO: 1})

	assert.Equal(t, GTIDSet{
		idStr: []Interval{
			{From: 1, To: 1},
		},
	}, s)

	s.Add(GTID{SID: id[:], GNO: 2})
	assert.Equal(t, GTIDSet{
		idStr: []Interval{
			{From: 1, To: 2},
		},
	}, s)

	s.Add(GTID{SID: id[:], GNO: 3})
	s.Add(GTID{SID: id[:], GNO: 4})
	assert.Equal(t, GTIDSet{
		idStr: []Interval{
			{From: 1, To: 4},
		},
	}, s)

	s.Add(GTID{SID: id[:], GNO: 6})
	assert.Equal(t, GTIDSet{
		idStr: []Interval{
			{From: 1, To: 4},
			{From: 6, To: 6},
		},
	}, s)

	s.Add(GTID{SID: id[:], GNO: 7})
	s.Add(GTID{SID: id[:], GNO: 8})
	assert.Equal(t, GTIDSet{
		idStr: []Interval{
			{From: 1, To: 4},
			{From: 6, To: 8},
		},
	}, s)

	s.Add(GTID{SID: id[:], GNO: 5})
	assert.Equal(t, GTIDSet{
		idStr: []Interval{
			{From: 1, To: 8},
		},
	}, s)
}

func TestGTIDSet__Single_SID__Grouping_In_Middle(t *testing.T) {
	s := GTIDSet{}
	id := uuid.New()
	idStr := string(id[:])

	s.Add(GTID{SID: id[:], GNO: 2})
	s.Add(GTID{SID: id[:], GNO: 3})
	s.Add(GTID{SID: id[:], GNO: 4})

	s.Add(GTID{SID: id[:], GNO: 20})

	s.Add(GTID{SID: id[:], GNO: 8})
	assert.Equal(t, GTIDSet{
		idStr: []Interval{
			{From: 2, To: 4},
			{From: 8, To: 8},
			{From: 20, To: 20},
		},
	}, s)

	s.Add(GTID{SID: id[:], GNO: 6})
	s.Add(GTID{SID: id[:], GNO: 7})

	assert.Equal(t, GTIDSet{
		idStr: []Interval{
			{From: 2, To: 4},
			{From: 6, To: 8},
			{From: 20, To: 20},
		},
	}, s)

	// Already Existed
	s.Add(GTID{SID: id[:], GNO: 8})
	assert.Equal(t, GTIDSet{
		idStr: []Interval{
			{From: 2, To: 4},
			{From: 6, To: 8},
			{From: 20, To: 20},
		},
	}, s)

	s.Add(GTID{SID: id[:], GNO: 19})
	s.Add(GTID{SID: id[:], GNO: 18})
	assert.Equal(t, GTIDSet{
		idStr: []Interval{
			{From: 2, To: 4},
			{From: 6, To: 8},
			{From: 18, To: 20},
		},
	}, s)
}

func TestGTIDSet__Single_SID__At_Beginning(t *testing.T) {
	s := GTIDSet{}
	id := uuid.New()
	idStr := string(id[:])

	s.Add(GTID{SID: id[:], GNO: 3})
	s.Add(GTID{SID: id[:], GNO: 4})
	s.Add(GTID{SID: id[:], GNO: 2})
	s.Add(GTID{SID: id[:], GNO: 1})

	assert.Equal(t, GTIDSet{
		idStr: []Interval{
			{From: 1, To: 4},
		},
	}, s)
}

func TestGTIDSet__Multi_SID(t *testing.T) {
	s := GTIDSet{}

	id1 := uuid.New()
	idStr1 := string(id1[:])

	id2 := uuid.New()
	idStr2 := string(id2[:])

	array := make([]int64, 0, 20)
	for i := 1; i <= 20; i++ {
		array = append(array, int64(i))
	}

	rand.Shuffle(20, func(i, j int) {
		array[j], array[i] = array[i], array[j]
	})
	for _, no := range array {
		s.Add(GTID{
			SID: id1[:],
			GNO: no,
		})
	}

	rand.Shuffle(20, func(i, j int) {
		array[j], array[i] = array[i], array[j]
	})
	for _, no := range array {
		s.Add(GTID{
			SID: id2[:],
			GNO: no,
		})
	}

	assert.Equal(t, GTIDSet{
		idStr1: []Interval{
			{From: 1, To: 20},
		},
		idStr2: []Interval{
			{From: 1, To: 20},
		},
	}, s)
}
