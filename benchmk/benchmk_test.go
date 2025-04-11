package benchmk

import (
	"fmt"
	"github.com/kebukeYi/TrainDB"
	"github.com/kebukeYi/TrainDB/common"
	"github.com/kebukeYi/TrainDB/lsm"
	"github.com/kebukeYi/TrainDB/model"
	"github.com/kebukeYi/TrainDB/utils"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
	"time"
)

//var benchMarkDir = "/usr/projects_gen_data/goprogendata/trainkvdata/test/benchmk"

var benchMarkDir = "F:\\ProjectsData\\golang\\TrainDB\\benchmk"

var benchMarkOpt = &lsm.Options{
	WorkDir:             benchMarkDir,
	MemTableSize:        10 << 20, // 10MB; 64 << 20(64MB)
	NumFlushMemtables:   10,       // 默认:15;
	BlockSize:           4 * 1024, // 4 * 1024;
	BloomFalsePositive:  0.01,     // 误差率;
	CacheNums:           1 * 1024, // 1024个
	ValueThreshold:      1 << 20,  // 1MB; 1 << 20(1MB)
	ValueLogMaxEntries:  300,      // 1000000
	ValueLogFileSize:    1 << 28,  // 256MB; 1<<30-1(1GB)
	VerifyValueChecksum: false,    // false

	MaxBatchCount: 1000,
	MaxBatchSize:  10 << 20, // 10 << 20(10MB)

	BaseTableSize:       2 << 20, // 2 << 20(2MB)
	TableSizeMultiplier: 2,
	BaseLevelSize:       8 << 20, // 8MB; 10 << 20(10MB)
	LevelSizeMultiplier: 8,       // 10
	NumCompactors:       3,       // 4

	NumLevelZeroTables: 5,
	MaxLevelNum:        common.MaxLevelNum,
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
	// go test -bench=BenchmarkNormalEntry -benchtime=3s -count=2 -failfast
	// go test -bench=BenchmarkNormalEntry -benchtime=100000x -count=5 -failfast
	b.ResetTimer()
	b.ReportAllocs()
	clearDir(benchMarkOpt.WorkDir)
	//traindb, _, _ := TrainDB.Open(benchMarkOpt)
	traindb, _, _ := TrainDB.Open(lsm.GetLSMDefaultOpt(benchMarkOpt.WorkDir))
	defer traindb.Close()

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key=%d", i))
		//valSize := 5 + 1 // val: 6B
		valSize := 127 + 1 // val: 12B
		//valSize := 10<<20 + 1 // val: 10.01MB
		//valSize := 64<<20 + 1 // val: 64.01MB
		err := traindb.Set(model.BuildBigEntry(key, uint64(valSize)))
		assert.Nil(b, err)
	}

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key=%d", i))
		_, err := traindb.Get(key)
		assert.Nil(b, err)

		key = []byte(randStr(18))
		_, err = traindb.Get(key)
		assert.Error(b, err)
	}
}

func BenchmarkWriteRequest(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	clearDir(benchMarkOpt.WorkDir)
	//traindb, _, _ := TrainDB.Open(benchMarkOpt)
	traindb, _, _ := TrainDB.Open(lsm.GetLSMDefaultOpt(benchMarkOpt.WorkDir))
	defer traindb.Close()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key=%d", i)
		val := fmt.Sprintf("val%d", i)
		//val := make([]byte, 10<<20+1)
		e := model.NewEntry([]byte(key), []byte(val))
		e.Key = model.KeyWithTs(e.Key)
		request := TrainDB.BuildRequest([]*model.Entry{e})
		if err := traindb.WriteRequest([]*TrainDB.Request{request}); err != nil {
			assert.Nil(b, err)
		} else {
			err := request.Wait()
			assert.Nil(b, err)
		}
	}

	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key=%d", i))
		_, err := traindb.Get(key)
		assert.Nil(b, err)

		key = []byte(randStr(18))
		_, err = traindb.Get(key)
		assert.Error(b, err)
	}
}

func TestNormalEntry(b *testing.T) {
	clearDir(benchMarkOpt.WorkDir)
	go utils.StartHttpDebugger()
	traindb, _, _ := TrainDB.Open(benchMarkOpt)
	//traindb, _, _ := TrainDB.Open(lsm.GetLSMDefaultOpt(benchMarkOpt.WorkDir))
	defer traindb.Close()
	n := 8000
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key=%d", i))
		//valSize := 5 + 1 // val: 6B
		valSize := 127 + 1 // val: 12B
		//valSize := 10<<20 + 1 // val: 10.01MB
		//valSize := 64<<20 + 1 // val: 64.01MB
		err := traindb.Set(model.BuildBigEntry(key, uint64(valSize)))
		assert.Nil(b, err)
	}

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key=%d", i))
		_, err := traindb.Get(key)
		assert.Nil(b, err)

		key = []byte(randStr(18))
		_, err = traindb.Get(key)
		assert.Error(b, err)
	}
}

func randStr(length int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	bytes := []byte(str)
	result := []byte{}
	rand.Seed(time.Now().UnixNano() + int64(rand.Intn(100)))
	for i := 0; i < length; i++ {
		result = append(result, bytes[rand.Intn(len(bytes))])
	}
	return string(result)
}
