package tsdb

import (
	"fmt"
	"testing"
)

func TestSkiplist_All(t *testing.T) {
	tree := newSkipList()
	tree.Add(2, "d")
	tree.Add(1, "a")
	tree.Add(2, "b")
	tree.Add(3, "c")
	tree.Add(4, "d")

	//nowDiskSegment := []string {"a", "b", "c"}

	iter := tree.All()
	for iter.Next() {
		fmt.Println(iter.Value().(string))
	}
}

func TestSkiplist_Range(t *testing.T) {
	tree := newSkipList()
	tree.Add(2, "d")
	tree.Add(1, "a")
	tree.Add(2, "b")
	tree.Add(3, "c")
	tree.Add(4, "d")

	//nowDiskSegment := []string {"a", "b", "c"}

	iter := tree.Range(3, 4)
	for iter.Next() {
		fmt.Println(iter.Value().(string))
	}
}
