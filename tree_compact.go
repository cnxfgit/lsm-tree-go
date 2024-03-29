package main

import (
	"fmt"
	"lsm/memtable"
	"math"
	"path"
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

// 将 memtable 的数据溢写落盘到 level0 层成为一个新的 sst 文件
func (t *Tree) flushMemTable(memTable memtable.MemTable) {
	// memtable 写到 level 0 层 sstable 中
	seq := t.levelToSeq[0].Load() + 1

	// 创建sst writer
	sstWriter, _ := NewSSTWriter(t.sstFile(0, seq), t.conf)
	defer sstWriter.Close()

	// 遍历 memtable 写入数据到 sst writer
	for _, kv := range memTable.All() {
		sstWriter.Appned(kv.Key, kv.Value)
	}

	// sstable 落盘
	size, blockToFilter, index := sstWriter.Finish()

	// 构造节点添加到 tree 的 node 中
	t.insertNode(0, seq, size, blockToFilter, index)
	// 尝试引发一轮 compact 操作
	t.tryTriggerCompact(0)
}

func (t *Tree) tryTriggerCompact(level int) {
	// 最后一层不执行 compact 操作
	if level == len(t.nodes)-1 {
		return
	}

	var size uint64
	for _, node := range t.nodes[level] {
		size += node.size
	}

	if size <= t.conf.SSTSize*uint64(math.Pow10(level))*uint64(t.conf.SSTNumPerLevel) {
		return
	}

	go func() {
		t.levelCompactC <- level
	}()
}

func (t *Tree) walFile() string {
	return path.Join(t.conf.Dir, "walfile", fmt.Sprintf("%d.wal", t.memTableIndex))
}
