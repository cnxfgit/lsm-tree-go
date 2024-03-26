package main

import (
	"bytes"
	"lsm/memtable"
	"lsm/wal"
	"os"
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

// 将只读 memtable 溢写落盘成为 level0 层 sstable 文件
func (t *Tree) compactMemTable(memCompactItem *memTableCompactItem) {
	// 处理 memtable 溢写工作:
	// 1 memtable 溢写到 0 层 sstable 中
	t.flushMemTable(memCompactItem.memTable)

	// 2 从 rOnly slice 中回收对应的 table
	t.dataLock.Lock()
	for i := 0; i < len(t.rOnlyMemTable); i++ {
		if t.rOnlyMemTable[i].memTable != memCompactItem.memTable {
			continue
		}
		t.rOnlyMemTable = t.rOnlyMemTable[i+1:]
	}
	t.dataLock.Unlock()

	// 3 删除相应的预写日志. 因为 memtable 落盘后数据已经安全，不存在丢失风险
	_ = os.Remove(memCompactItem.walFile)
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

func (t *Tree) insertNode(level int, seq int32, size uint64, blockToFilter map[uint64][]byte, index []*Index) {
	file := t.sstFile(level, seq)
	sstReader, _ := NewSSTReader(file, t.conf)

	t.insertNodeWithReader(sstReader, level, seq, size, blockToFilter, index)
}

// 插入一个 node 到指定 level 层
func (t *Tree) insertNodeWithReader(sstReader *SSTReader, level int, seq int32,
	size uint64, blockToFilter map[uint64][]byte, index []*Index) {
	file := t.sstFile(level, seq)
	// 记录当前 level 层对应的 seq 号
	t.levelToSeq[level].Store(seq)

	// 创建一个lsm node
	newNode := NewNode(t.conf, file, sstReader, level, seq, size, blockToFilter, index)
	// 对于 level0 而言，只需要 append 插入 node 即可
	if level == 0 {
		t.levelLocks[0].Lock()
		t.nodes[level] = append(t.nodes[level], newNode)
		t.levelLocks[0].Unlock()
		return
	}

	// 对于 level1~levelk 层，需要根据 node 中 key 的大小，遵循顺序插入
	for i := 0; i < len(t.nodes[level])-1; i++ {
		// 遵循从小到大的遍历顺序，找到首个最小 key 比 newNode 最大 key 还大的 node，将 newNode 插入在其之前
		if bytes.Compare(newNode.End(), t.nodes[level][i+1].Start()) < 0 {
			t.levelLocks[level].Lock()
			t.nodes[level] = append(t.nodes[level][:i+1], t.nodes[level][i:]...)
			t.nodes[level][i+1] = newNode
			t.levelLocks[level].Unlock()
			return
		}
	}

	// 遍历完 level 层所有节点都还没插入 newNode，说明 newNode 是该层 key 值最大的节点，则 append 到最后即可
	t.levelLocks[level].Lock()
	t.nodes[level] = append(t.nodes[level], newNode)
	t.levelLocks[level].Unlock()
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

// 针对 level 层进行排序归并操作
func (t *Tree) compactLevel(level int) {
    // 获取到 level 和 level + 1 层内需要进行本次归并的节点
    pickedNodes := t.pickCompactNodes(level)


    // 插入到 level + 1 层对应的目标 sstWriter
    seq := t.levelToSeq[level+1].Load() + 1
    sstWriter, _ := NewSSTWriter(t.sstFile(level+1, seq), t.conf)
    defer sstWriter.Close()


    // 获取 level + 1 层每个 sst 文件的大小阈值
    sstLimit := t.conf.SSTSize * uint64(math.Pow10(level+1))
    // 获取本次排序归并的节点涉及到的所有 kv 数据
    pickedKVs := t.pickedNodesToKVs(pickedNodes)
    // 遍历每笔需要归并的 kv 数据
    for i := 0; i < len(pickedKVs); i++ {
        // 倘若新生成的 level + 1 层 sst 文件大小已经超限
        if sstWriter.Size() > sstLimit {
            // 将 sst 文件溢写落盘
            size, blockToFilter, index := sstWriter.Finish()
            // 将 sst 文件对应 node 插入到 lsm tree 内存结构中
            t.insertNode(level+1, seq, size, blockToFilter, index)
            // 构造一个新的 level + 1 层 sstWriter
            seq = t.levelToSeq[level+1].Load() + 1
            sstWriter, _ = NewSSTWriter(t.sstFile(level+1, seq), t.conf)
            defer sstWriter.Close()
        }


        // 将 kv 数据追加到 sstWriter
        sstWriter.Append(pickedKVs[i].Key, pickedKVs[i].Value)
        // 倘若这是最后一笔 kv 数据，需要负责把 sstWriter 溢写落盘并把对应 node 插入到 lsm tree 内存结构中
        if i == len(pickedKVs)-1 {
            size, blockToFilter, index := sstWriter.Finish()
            t.insertNode(level+1, seq, size, blockToFilter, index)
        }
    }


    // 移除这部分被合并的节点
    t.removeNodes(level, pickedNodes)


    // 尝试触发下一层的 compact 操作
    t.tryTriggerCompact(level + 1)
}

func (t *Tree) newMemTable() {
	t.walWriter, _ = wal.NewWaALWriter(t.walFile())
	t.memTable = t.conf.MemTableConstructor()
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
