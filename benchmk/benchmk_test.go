package benchmk

import (
	"fmt"
	"github.com/kebukeYi/TrainDB"
	"github.com/kebukeYi/TrainDB/common"
	"github.com/kebukeYi/TrainDB/lsm"
	"github.com/kebukeYi/TrainDB/model"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var benchMarkDir = "F:\\ProjectsData\\golang\\TrainDB\\benchmk"

var benchMarkOpt = &lsm.Options{
	WorkDir:             benchMarkDir,
	MemTableSize:        10 << 20, // 10MB; 64 << 20(64MB)
	NumFlushMemtables:   10,       // 默认:15;
	SSTableMaxSz:        10 << 20, // 10MB; 64 << 20(64MB)
	BlockSize:           4 * 1024, // 4 * 1024;
	BloomFalsePositive:  0.01,     // 误差率;
	CacheNums:           1 * 1024, // 10240个
	ValueThreshold:      1 << 20,  // 1MB; 1 << 20(1MB)
	ValueLogMaxEntries:  10000,    // 1000000
	ValueLogFileSize:    1 << 29,  // 512MB; 1<<30-1(1GB)
	VerifyValueChecksum: false,    // false

	MaxBatchCount: 1000,
	MaxBatchSize:  10 << 20, // 10 << 20(10MB)

	NumCompactors:       3,       // 4
	BaseLevelSize:       8 << 20, //8MB; 10 << 20(10MB)
	LevelSizeMultiplier: 2,       // 10
	TableSizeMultiplier: 2,
	BaseTableSize:       5 << 20, // 2 << 20(2MB)
	NumLevelZeroTables:  5,
	MaxLevelNum:         common.MaxLevelNum,
}

func clearDir(dir string) {
	_, err := os.Stat(dir)
	if err == nil {
		if err = os.RemoveAll(dir); err != nil {
			common.Panic(err)
		}
	}
	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		_ = fmt.Sprintf("create dir %s failed", dir)
	}
}

func BenchmarkNormalEntry(b *testing.B) {
	// go test -bench=BenchmarkBigEntry -benchtime=5s -count=5  # 运行5次，每次至少5秒
	// go test -bench=BenchmarkBigEntry -benchtime=100000x  # 固定运行100,000次
	b.ResetTimer()
	b.ReportAllocs()
	clearDir(benchMarkOpt.WorkDir)
	//traindb, _, _ := TrainDB.Open(benchMarkOpt)
	traindb, _, _ := TrainDB.Open(lsm.GetLSMDefaultOpt(benchMarkOpt.WorkDir))
	defer traindb.Close()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key=%d", i))
		err := traindb.Set(model.BuildKeyEntry(key)) // val : 128B
		assert.Nil(b, err)
	}
}

func BenchmarkBigEntry(b *testing.B) {
	// go test -bench=BenchmarkBigEntry -benchtime=5s -count=5  # 运行5次，每次至少5秒
	// go test -bench=BenchmarkBigEntry -benchtime=100000x  # 固定运行100,000次
	b.ResetTimer()
	b.ReportAllocs()
	clearDir(benchMarkOpt.WorkDir)
	//traindb, _, _ := TrainDB.Open(benchMarkOpt)
	traindb, _, _ := TrainDB.Open(lsm.GetLSMDefaultOpt(benchMarkOpt.WorkDir))
	defer traindb.Close()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key=%d", i))
		valSize := 10<<20 + 1 // val 10.01MB
		err := traindb.Set(model.BuildBigEntry(key, uint64(valSize)))
		assert.Nil(b, err)
	}
}

func BenchmarkMoreBigEntry(b *testing.B) {
	// go test -bench=BenchmarkMoreBigEntry -benchtime=5s -count=5  # 运行5次，每次至少5秒
	// go test -bench=BenchmarkMoreBigEntry -benchtime=100000x  # 固定运行100,000次
	b.ResetTimer()
	b.ReportAllocs()
	clearDir(benchMarkOpt.WorkDir)
	//traindb, _, _ := TrainDB.Open(benchMarkOpt)
	traindb, _, _ := TrainDB.Open(lsm.GetLSMDefaultOpt(benchMarkOpt.WorkDir))
	defer traindb.Close()

	fmt.Println(b.N)
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key=%d", i))
		valSize := 64<<20 + 1 // val 10.01MB
		err := traindb.Set(model.BuildBigEntry(key, uint64(valSize)))
		assert.Nil(b, err)
	}
}
