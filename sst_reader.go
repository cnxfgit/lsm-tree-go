package main

import (
	"bufio"
	"os"
)

type SSTReader struct {
	conf         *Config       // 配置文件
	src          *os.File      // 对应的文件
	reader       *bufio.Reader // 读取文件的 reader
	filterOffset uint64        // 过滤器块起始位置在 sstable 的 offset
	filterSize   uint64        // 过滤器块的大小，单位 byte
	indexOffset  uint64        // 索引块起始位置在 sstable 的 offset
	indexSize    uint64        // 索引块的大小，单位 byte
}
