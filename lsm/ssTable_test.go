package lsm

import (
	"fmt"
	"github.com/kebukeYi/TrainDB/common"
	"github.com/kebukeYi/TrainDB/model"
	"github.com/kebukeYi/TrainDB/utils"
	"os"
	"strconv"
	"testing"
)

func TestOpenSStable(t *testing.T) {
	//tableName := "/user/trainFS/temp/nameNode1/task/00005.sst"
	tableName := "F:\\ProjectsData\\golang\\TrainDB\\test\\db\\00001.sst"
	options := GetLSMDefaultOpt("")
	fid := utils.FID(tableName)
	levelManger := &LevelsManger{}
	levelManger.cache = newLevelsCache(options)
	table := &Table{lm: levelManger, fid: fid, Name: strconv.FormatUint(fid, 10) + SSTableName}
	table.sst = OpenSStable(&utils.FileOptions{
		FileName: tableName,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    0,
		FID:      table.fid,
	})
	table.IncrRef()
	if err := table.sst.Init(); err != nil {
		common.Err(err)
	}
	iterator := table.NewTableIterator(&model.Options{IsAsc: true})
	iterator.Rewind()
	for iterator.Valid() {
		entry := iterator.Item().Item
		fmt.Printf("key=%s, value=%s, meta:%d, version=%d \n",
			model.ParseKey(entry.Key), entry.Value, entry.Meta, entry.Version)
		iterator.Next()
	}
}
