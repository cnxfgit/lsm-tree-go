package main

import (
	"lsm/memtable"
	"strconv"
	"strings"
)

type memTableCompactItem struct {
	walFile  string
	memTable memtable.MemTable
}

func walFileToMemTableIndex(walFile string) int {
	rawIndex := strings.Replace(walFile, ".wal", "", -1)
	index, _ := strconv.Atoi(rawIndex)
	return index
}
