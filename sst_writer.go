package main

import (
	"bytes"
	"os"
)

type SSTWriter struct {
	conf          *Config           // 配置文件
	dest          *os.File          // sstable 对应的磁盘文件
	dataBuf       *bytes.Buffer     // 数据块缓冲区 key -> val
	filterBUf     *bytes.Buffer     // 过滤器块缓冲区 prev block offset -> filter bit map
	indexBuf      *bytes.Buffer     // 索引块缓冲区 index key -> prev block offset, prev block size
	blockToFilter map[uint64][]byte // prev block offset -> filter bit map
	index         []*Index          // index key -> prev block offset, prev block size

	dataBlock     *Block   // 数据块
	filterBlock   *Block   // 过滤器块
	indexBlock    *Block   // 索引块
	assistScratch [20]byte // 用于在写索引块时临时使用的辅助缓冲区

	prevKey         []byte // 前一笔数据的 key
	prevBlockOffset uint64 // 前一个数据块的起始偏移位置
	prevBlockSize   uint64 // 前一个数据块的大小
}

// sstable 中用于快速检索 block 的索引
type Index struct {
	Key             []byte // 索引的 key. 保证其 >= 前一个 block 最大 key； < 后一个 block 的最小 key
	PrevBlockOffset uint64 // 索引前一个 block 起始位置在 sstable 中对应的 offset
	PrevBlockSize   uint64 // 索引前一个 block 的大小，单位 byte
}
