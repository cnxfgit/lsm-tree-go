package main

import "lsm/memtable"

type memTableCompactItem struct {
	walFile  string
	memTable memtable.MemTable
}