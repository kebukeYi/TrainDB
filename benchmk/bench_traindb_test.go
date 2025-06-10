package benchmk

import (
	"fmt"
	"github.com/kebukeYi/TrainDB"
	"github.com/kebukeYi/TrainDB/lsm"
	"github.com/kebukeYi/TrainDB/model"
	"testing"
)

var triandb *TrainDB.TrainKVDB

func initTrainDB() {
	fmt.Println("init TrainDB")
	//dir := "F:\\ProjectsData\\golang\\TrainDB\\benchmk"
	dir := "/usr/golanddata/triandb/benchmk1"
	clearDir(dir)
	trianDB, err, _ := TrainDB.Open(lsm.GetLSMDefaultOpt(dir))
	if err != nil {
		panic(err)
	}
	triandb = trianDB
}

func Benchmark_PutValue_TrainDB(b *testing.B) {
	initTrainDB()
	defer triandb.Close()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		entry := model.NewEntry(GetKey(i), GetValue())
		err := triandb.Set(entry)
		if err != nil {
			panic(err)
			return
		}
	}
}

func initTrainDBData() {
	for i := 0; i < 500000; i++ {
		entry := model.NewEntry(GetKey(i), GetValue())
		err := triandb.Set(entry)
		if err != nil {
			panic(err)
			return
		}
	}
}

func Benchmark_GetValue_TrainDB(b *testing.B) {
	initTrainDB()
	initTrainDBData()
	defer triandb.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := triandb.Get(GetKey(i))
		if err != nil {
			panic(err)
		}
	}
}
