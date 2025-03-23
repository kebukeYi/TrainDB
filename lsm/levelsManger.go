package lsm

import (
	"github.com/kebukeYi/TrainDB/common"
	"github.com/kebukeYi/TrainDB/model"
	"github.com/kebukeYi/TrainDB/utils"
	"strconv"
	"sync"
	"sync/atomic"
)

type levelsManger struct {
	maxFID           atomic.Uint64   // sst 已经分配出去的最大fid,只要创建了 memoryTable 就算已分配;
	levelHandlers    []*levelHandler // 每层的处理器
	opt              *Options
	lsm              *LSM          // 上层引用
	cache            *LevelsCache  // 缓存 block 和 sst.index() 数据
	manifestFile     *ManifestFile // 增删 sst 元信息
	compactIngStatus *compactIngStatus
}

func (lm *levelsManger) nextFileID() uint64 {
	id := lm.maxFID.Add(1)
	return id
}

func (lsm *LSM) InitLevelManger(opt *Options) *levelsManger {
	lm := &levelsManger{
		lsm: lsm,
		opt: opt,
	}
	lm.compactIngStatus = lsm.newCompactStatus()
	if err := lm.loadManifestFile(); err != nil {
		common.Panic(err)
	}
	if err := lm.build(); err != nil {
		common.Panic(err)
	}
	return lm
}

func (lm *levelsManger) loadManifestFile() (err error) {
	lm.manifestFile, err = OpenManifestFile(&utils.FileOptions{Dir: lm.opt.WorkDir})
	return err
}

func (lm *levelsManger) build() error {
	lm.levelHandlers = make([]*levelHandler, lm.opt.MaxLevelNum)
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levelHandlers[i] = &levelHandler{
			mux:            sync.RWMutex{},
			levelID:        i,
			tables:         make([]*table, 0),
			totalSize:      0,
			totalStaleSize: 0,
			lm:             lm,
		}
	}

	manifest := lm.manifestFile.GetManifest()

	if err := lm.manifestFile.checkSSTable(utils.LoadIDMap(lm.opt.WorkDir)); err != nil {
		return err
	}

	lm.cache = newLevelsCache(lm.opt)

	var maxFID uint64
	for fid, tableInfo := range manifest.Tables {
		filePathName := utils.FileNameSSTable(lm.opt.WorkDir, fid)
		if fid > maxFID {
			maxFID = fid
		}
		t, _ := openTable(lm, filePathName, nil)
		lm.levelHandlers[tableInfo.LevelID].add(t)
		lm.levelHandlers[tableInfo.LevelID].addSize(t)
	}

	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levelHandlers[i].Sort()
	}

	if maxFID > lm.maxFID.Load() {
		lm.maxFID.Store(maxFID)
	}
	return nil
}

func (lm *levelsManger) lastLevel() *levelHandler {
	return lm.levelHandlers[len(lm.levelHandlers)-1]
}

func (lm *levelsManger) iterators(opt *model.Options) []model.Iterator {
	iters := make([]model.Iterator, 0)
	for _, handler := range lm.levelHandlers {
		iters = append(iters, handler.iterators(opt)...)
	}
	return iters
}

func (lm *levelsManger) Get(keyTs []byte) (model.Entry, error) {
	var (
		entry model.Entry
		err   error
	)
	entry.Version = -1
	if entry, err = lm.levelHandlers[0].Get(keyTs); entry.Version != -1 {
		return entry, err
	}
	for i := 1; i < lm.opt.MaxLevelNum; i++ {
		if entry, err = lm.levelHandlers[i].Get(keyTs); entry.Version != -1 {
			return entry, err
		}
	}
	return entry, common.ErrKeyNotFound
}

func (lm *levelsManger) checkOverlap(tables []*table, lev int) bool {
	kr := getKeyRange(tables...) // 给定的 table 区间
	for i, lh := range lm.levelHandlers {
		if i < lev { // 跳过 低于本层的;
			continue
		}
		lh.mux.RLock()
		// 判断当前 level 是否存在区间;
		left, right := lh.findOverLappingTables(levelHandlerRLocked{}, kr)
		lh.mux.RUnlock()
		if right-left > 0 {
			return true
		}
	}
	return false
}

func (lm *levelsManger) flush(imm *memoryTable) (err error) {
	fid := imm.wal.Fid()
	sstName := utils.FileNameSSTable(lm.opt.WorkDir, fid)

	builder := newSSTBuilder(lm.opt)
	skipListIterator := imm.skipList.NewSkipListIterator(strconv.FormatUint(fid, 10) + MemTableName)
	defer skipListIterator.Close() // 涉及到 immemoryTable的清除和相关 wal的清理;
	for skipListIterator.Rewind(); skipListIterator.Valid(); skipListIterator.Next() {
		entry := skipListIterator.Item().Item
		builder.add(entry, false)
	}

	// 此时磁盘中已经生成 .sst 文件;
	t, _ := openTable(lm, sstName, builder)
	// 向manifest 中添加, 添加失败了呢?
	// 假设5.wal 刚转化成 5.sst, 那么5.wal理应被删除掉;但是和5.wal绑定的跳表正在被引用,因此无法直接删除掉5.wal;
	// 随后 系统突然宕机关闭, 重启时, 会先加载 .sst文件, 然后再加载.wal; 那么就会出现 5.wal 和 5.sst 的重叠;
	// 因此, 在 wal 刷盘时, 等于 .sst 的最大文件ID的 .wal 文件需要删除掉;
	err = lm.manifestFile.AddTableMeta(0, &TableMeta{
		ID:       fid,
		Checksum: []byte{'s', 'k', 'i', 'p'},
	})
	common.Panic(err)
	lm.levelHandlers[0].add(t)
	// fmt.Printf("flush sstable %d.sst; \n", fid)
	return nil
}

func (lm *levelsManger) close() error {
	if err := lm.manifestFile.Close(); err != nil {
		return err
	}
	for i := range lm.levelHandlers {
		if err := lm.levelHandlers[i].close(); err != nil {
			return err
		}
	}
	return nil
}
