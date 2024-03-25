package main

import "bytes"

type Block struct {
	conf       *Config
	buffer     [30]byte      // 进行数据转移时使用的临时缓冲区
	record     *bytes.Buffer // 记录全量数据的缓冲区
	entriesCnt int           // kv 对数量
	prevKey    []byte        // 最晚一笔写入的数据的 key
}
