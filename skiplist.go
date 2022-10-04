package tsdb

import (
	"math"
	"math/rand"
)

type skiplistLevel struct {
	forward *skiplistNode
}

type skiplistNode struct {
	value    interface{}
	key      int64
	backward *skiplistNode
	level    []skiplistLevel
}

type skiplist struct {
	header *skiplistNode
	tail   *skiplistNode
	length uint32
	level  int
}

const (
	SKIPLIST_MAXLEVEL = 32
	SKIPLIST_P        = 0.25
)

func newSkipListNode(level int, key int64, value interface{}) *skiplistNode {
	return &skiplistNode{
		key:   key,
		value: value,
		level: make([]skiplistLevel, level),
	}
}

func newSkipList() List {
	sl := &skiplist{}
	sl.level = 1
	sl.length = 0
	sl.header = newSkipListNode(SKIPLIST_MAXLEVEL, 0, nil)
	for j := 0; j < SKIPLIST_MAXLEVEL; j++ {
		sl.header.level[j].forward = nil
	}
	sl.header.backward = nil
	sl.tail = nil
	return sl
}

func (sl *skiplist) Search(key int64) *skiplistNode {
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && x.level[i].forward.key < key {
			x = x.level[i].forward
		}
	}
	if x.level[0].forward != nil && x.level[0].forward.key == key {
		return x.level[0].forward
	}
	return nil
}

func (sl *skiplist) skiplistRandomLevel() int {
	level := 1
	for rand.Float64() < SKIPLIST_P && level < SKIPLIST_MAXLEVEL {
		level += 1
	}
	return level
}

func (sl *skiplist) Add(key int64, value interface{}) {
	var (
		update []*skiplistNode
		x      *skiplistNode
		i      int
	)
	update = make([]*skiplistNode, SKIPLIST_MAXLEVEL)
	x = sl.header
	for i = sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && x.level[i].forward.key < key {
			x = x.level[i].forward
		}
		update[i] = x
	}

	//if the same key already exists, only update the value
	if x.level[0].forward != nil && x.level[0].forward.key == key {
		x.level[0].forward.value = value
		return
	}

	//initialize high level
	level := sl.skiplistRandomLevel()
	if level > sl.level {
		for i = sl.level; i < level; i++ {
			update[i] = sl.header
		}
		sl.level = level
	}

	x = newSkipListNode(level, key, value)

	//at "the lower level", insert new skiplistNode
	for i = 0; i < level; i++ {
		x.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = x
	}

	//set backward
	if update[0] == sl.header {
		x.backward = nil
	} else {
		x.backward = update[0]
	}

	//set forward's backword/tail of the skiplist
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x
	} else {
		sl.tail = x
	}
	sl.length++
}

func (sl *skiplist) Remove(key int64) bool {
	var (
		update []*skiplistNode
		x      *skiplistNode
		i      int
	)
	update = make([]*skiplistNode, SKIPLIST_MAXLEVEL)

	//find the target key
	x = sl.header
	for i = sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && x.level[i].forward.key < key {
			x = x.level[i].forward
		}
		update[i] = x
	}

	x = x.level[0].forward
	//key found
	if x != nil && key == x.key {
		for i = 0; i < sl.level; i++ {
			if update[i].level[i].forward == x {
				update[i].level[i].forward = x.level[i].forward
			}
		}
		if x.level[0].forward != nil {
			x.level[0].forward.backward = x.backward
		} else {
			sl.tail = x.backward
		}

		for sl.level > 1 && sl.header.level[sl.level-1].forward == nil {
			sl.level--
		}
		sl.length--
		return true
	}
	//key not found
	return false
}

func (sl *skiplist) Range(start, end int64) Iter {
	iter := &skiplistRangeIter{
		slRange: skiplistRange{
			start: start,
			end:   end,
			sl:    sl,
		},
		curr: nil,
	}
	iter.init()
	return iter
}

func (sl *skiplist) All() Iter {
	return sl.Range(0, math.MaxInt64)
}

//===============skiplistIterator begin

type skiplistRange struct {
	start int64
	end   int64
	sl    *skiplist
}

type skiplistRangeIter struct {
	slRange skiplistRange
	curr    *skiplistNode
}

func (iter *skiplistRangeIter) init() {
	iter.curr = iter.slRange.sl.header
	sl := iter.slRange.sl
	for i := sl.level; i >= 0; i-- {
		for iter.curr.level[i].forward != nil && iter.curr.level[i].forward.key < iter.slRange.start {
			iter.curr = iter.curr.level[i].forward
		}
	}
}

func (iter *skiplistRangeIter) Next() bool {
	iter.curr = iter.curr.level[0].forward
	return iter.curr != nil && iter.curr.key <= iter.slRange.end
}

func (iter *skiplistRangeIter) Value() interface{} {
	return iter.curr.value
}
