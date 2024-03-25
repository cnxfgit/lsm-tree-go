package main

type Node struct {
	conf          *Config           // 配置文件
	file          string            // sstable 对应的文件名，不含目录路径
	level         int               // sstable 所在 level 层级
	seq           int32             // sstable 的 seq 序列号. 对应为文件名中的 level_seq.sst 中的 seq
	size          uint64            // sstable 的大小，单位 byte
	blockToFilter map[uint64][]byte // 各 block 对应的 filter bitmap
	index         []*Index          // 各 block 对应的索引
	startKey      []byte            // sstable 中最小的 key
	endKey        []byte            // sstable 中最大的 key
	sstReader     *SSTReader        // 读取 sst 文件的 reader 入口
}
