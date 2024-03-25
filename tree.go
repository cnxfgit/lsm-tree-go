package main

import (
	"lsm/memtable"
	"lsm/wal"
	"os"
	"path"
	"sync"
	"sync/atomic"
)

type Tree struct {
	conf *Config

	dataLock   sync.RWMutex   // 读写数据时使用的锁
	levelLocks []sync.RWMutex // 每层 node 节点使用的读写锁

	memTable      memtable.MemTable      // 读写 memtable
	rOnlyMemTable []*memTableCompactItem // 只读 memtable

	walWriter *wal.WALWriter // 预写日志写入口
	nodes     [][]*Node      // lsm树状数据结构

	memCompactC   chan *memTableCompactItem // memtable 达到阈值时，通过该 chan 传递信号，进行溢写工作
	levelCompactC chan int                  // 某层 sst 文件大小达到阈值时，通过该 chan 传递信号，进行溢写工作
	stopc         chan struct{}             // lsm tree 停止时通过该 chan 传递信号

	memTableIndex int            // memtable index，需要与 wal 文件一一对应
	levelToSeq    []atomic.Int32 // 各层 sstable 文件 seq. sstable 文件命名为 level_seq.sst
}

// 构建出一棵 lsm tree
func NewTree(conf *Config) (*Tree, error) {
	// 1 构造 lsm tree 实例
	t := Tree{
		conf:          conf,
		memCompactC:   make(chan *memTableCompactItem),
		levelCompactC: make(chan int),
		stopc:         make(chan struct{}),
		levelToSeq:    make([]atomic.Int32, conf.MaxLevel),
		nodes:         make([][]*Node, conf.MaxLevel),
		levelLocks:    make([]sync.RWMutex, conf.MaxLevel),
	}

	// 2 读取 sst 文件，还原出整棵树
	if err := t.constructTree(); err != nil {
		return nil, err
	}

	// 3 运行 lsm tree 压缩调整协程
	go t.compact()

	// 4 读取 wal 还原出 memtable
	if err := t.constructMemtable(); err != nil {
		return nil, err
	}

	// 5 返回 lsm tree 实例
	return &t, nil
}



// 读取 wal 还原出 memtable
func (t *Tree) constructMemtable() error {
	// 1 读 wal 目录，获取所有的 wal 文件
	wals, _ := os.ReadDir(path.Join(t.conf.Dir, "walfile"))

	// 2 倘若 wal 目录不存在或者 wal 文件不存在，则构造一个新的 memtable
	if len(wals) == 0 {
		t.newMemTable()
		return nil
	}

	// 3 依次还原 memtable. 最晚一个 memtable 作为读写 memtable
	// 前置 memtable 作为只读 memtable，分别添加到内存 slice 和 channel 中.
	return t.restoreMemTable(wals)
}

// 运行 compact 协程.
func (t *Tree) compact() {
	for {
		select {
		// 接收到 lsm tree 终止信号，退出协程.
		case <-t.stopc:
			return
		// 接收到 read-only memtable，需要将其溢写到磁盘成为 level0 层 sstable 文件.
		case memCompactItem := <-t.memCompactC:
			t.compactMemTable(memCompactItem)
		// 接收到 level 层 compact 指令，需要执行 level~level+1 之间的 level sorted merge 流程.
		case level := <-t.levelCompactC:
			t.compactLevel(level)
		}
	}
}

// 写入一组 kv 对到 lsm tree. 会直接写入到读写 memtable 中.
func (t *Tree) Put(key, value []byte) error {
	// 1 加写锁
	t.dataLock.Lock()
	defer t.dataLock.Unlock()

	// 2 数据预写入预写日志中，防止因宕机引起 memtable 数据丢失.
	if err := t.walWriter.Write(key, value); err != nil {
		return err
	}

	// 3 数据写入读写跳表
	t.memTable.Put(key, value)

	// 4 倘若读写跳表的大小未达到 level0 层 sstable 的大小阈值，则直接返回.
	// 考虑到溢写成 sstable 后，需要有一些辅助的元数据，预估容量放大为 5/4 倍
	if uint64(t.memTable.Size()*5/4) <= t.conf.SSTSize {
		return nil
	}

	// 5 倘若读写跳表数据量达到上限，则需要切换跳表
	t.refreshMemTableLocked()

	return nil
}

// 切换读写跳表为只读跳表，并构建新的读写跳表
func (t *Tree) refreshMemTableLocked() {
	// 将读写跳表切换为只读跳表，追加到 slice 中，并通过 chan 发送给 compact 协程，由其负责进行溢写成为 level0 层 sst 文件的操作.
	oldItem := memTableCompactItem{
		walFile:  t.walFile(),
		memTable: t.memTable,
	}

	t.rOnlyMemTable = append(t.rOnlyMemTable, &oldItem)
	t.walWriter.Close()

	go func() {
		t.memCompactC <- &oldItem
	}()

	// 构造一个新的读写 memtable，并构造与之相应的 wal 文件.
	t.memTableIndex++
	t.newMemTable()
}

// 根据 key 读取数据.
func (t *Tree) Get(key []byte) ([]byte, bool, error) {
	t.dataLock.RLock()
	// 1 首先读 active memtable.
	value, ok := t.memTable.Get(key)
	if ok {
		t.dataLock.RUnlock()
		return value, true, nil
	}

	// 2 读 readOnly memtable
	for i := len(t.rOnlyMemTable) - 1; i >= 0; i-- {
		value, ok = t.rOnlyMemTable[i].memTable.Get(key)
		if ok {
			t.dataLock.RUnlock()
			return value, true, nil
		}
	}
	t.dataLock.RUnlock()

	// 3 读 sstable level0 层.
	var err error
	t.levelLocks[0].RLock()
	for i := len(t.nodes[0]) - 1; i >= 0; i-- {
		if value, ok, err = t.nodes[0][i].Get(key); err != nil {
			t.levelLocks[0].RUnlock()
			return nil, false, err
		}
		if ok {
			t.levelLocks[0].RUnlock()
			return value, true, nil
		}
	}
	t.levelLocks[0].RUnlock()

	// 4 依次读 sstable level 1 ~ i 层.
	for level := 1; level < len(t.nodes); level++ {
		t.levelLocks[level].RLock()
		node, ok := t.levelBinarySearch(level, key, 0, len(t.nodes[level])-1)
		if !ok {
			t.levelLocks[level].RUnlock()
			continue
		}
		if value, ok, err = node.Get(key); err != nil {
			t.levelLocks[level].RUnlock()
			return nil, false, err
		}
		if ok {
			t.levelLocks[level].RUnlock()
			return value, true, nil
		}
		t.levelLocks[level].RUnlock()
	}

	// 5 至此都没有读到数据，则返回 key 不存在.
	return nil, false, nil
}
