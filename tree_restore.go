package main

import (
	"io/fs"
	"os"
	"sort"
	"strings"

	"golang.org/x/tools/go/analysis/passes/nilfunc"
)

// 读取 sst 文件，还原出整棵树
func (t *Tree) constructTree() error {
	// 读取sst文件目录下的sst文件列表
	sstEntries, err := t.getSortedSSTEntries()
	if err != nil {
		return err
	}

	// 遍历每个 sst 文件，将其加载为 node 添加 lsm tree 的 nodes 内存切片中
	for _, sstEntry := range sstEntries {
		if err = t.loadNode(sstEntry); err != nil {
			return err
		}
	}

	return nil
}

func (t *Tree) getSortedSSTEntries() ([]fs.DirEntry, error) {
	allEntries, err := os.ReadDir(t.conf.Dir)
	if err != nil {
		return nil, err
	}

	sstEntries := make([]fs.DirEntry, 0, len(allEntries))
	for _, entry := range allEntries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".sst") {
			continue
		}
		sstEntries = append(sstEntries, entry)
	}

	sort.Slice(sstEntries, func(i, j int) bool {
		levelI, seqI := getLevelSeqFromSSTFile(sstEntries[i].Name())
		levelJ, seqJ := getLevelSeqFromSSTFile(sstEntries[j].Name())
		if levelI == levelJ {
			return seqI < seqJ
		}
		return levelI < levelJ
	})

	return sstEntries, nil
}

// 将一个 sst 文件作为一个 node 加载进入 lsm tree 的拓扑结构中
func (t *Tree) loadNode(sstEntry fs.DirEntry) error {
	// 创建 sst 文件对应的reader
	sstReader, err := NewSSTReader(sstEntry.Name(), t.conf)
	if err != nil {
		return err
	}

	// 读取各block块对应的filter信息
	blockToFilter, err := sstReader.ReadFilter()
	if err != nil {
		return err
	}

	// 读取index 信息
	index, err := sstReader.ReadIndex()
	if err != nil {
		return err
	}

	// 获取sst文件的大小， 单位byte
	size, err := sstReader.Size()
	if err != nil {
		return err
	}

	// 解析 sst 文件名，得知 sst 文件对应的 level 以及 seq 号
	level, seq := getLevelSeqFromSSTFile(sstEntry.Name())
	// 将 sst 文件作为一个 node 插入到 lsm tree 中
	t.insertNodeWithReader(sstReader, level, seq, size, blockToFilter, index)
	return nil
}
