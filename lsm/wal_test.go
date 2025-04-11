package lsm

import (
	"fmt"
	"github.com/kebukeYi/TrainDB/model"
	"github.com/kebukeYi/TrainDB/utils"
	"math/rand"
	"testing"
	"time"
)

var walTestPath = "/usr/projects_gen_data/goprogendata/trainkvdata/test/wal"

//var walTestPath = "F:\\ProjectsData\\golang\\TrainDB\\test\\wal"

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

func RandString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		b := r.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return string(bytes)
}

func getDefaultFileOpt(walFilePath string) *utils.FileOptions {
	opt := GetLSMDefaultOpt("")
	options := &utils.FileOptions{
		FID:      0,
		FileName: walFilePath,
		Dir:      walFilePath,
		Path:     walFilePath,
		MaxSz:    int32(opt.MemTableSize),
	}
	//options.FileName = filepath.Join(walTestPath, options.FileName)
	return options
}
func TestWAL_WalDecode(t *testing.T) {
	walFilePath := "/user/trainFS/temp/nameNode1/task/00005.wal"
	w := OpenWalFile(getDefaultFileOpt(walFilePath))
	reader := model.NewHashReader(w.file.Fd)
	var readAt uint32 = 0
	for {
		var entry *model.Entry
		entry, readAt = w.Read(reader)
		if readAt > 0 {
			fmt.Printf("entry: key:%s val:%s meta:%d version:%d \n",
				model.ParseKey(entry.Key), entry.Value, entry.Meta, model.ParseTsVersion(entry.Key))
		} else {
			break
		}
	}
}

func TestWAL_Write(t *testing.T) {
	w := OpenWalFile(getDefaultFileOpt(""))
	for i := 0; i < 10; i++ {
		var entry = model.Entry{
			Key:       []byte(RandString(10)),
			Value:     []byte(RandString(10)),
			Meta:      1,
			ExpiresAt: uint64(time.Now().Unix()),
		}
		err := w.Write(&entry)
		if err != nil {
			fmt.Printf("write failed, err: %v\n", err)
		}
	}
	w.file.Sync()
	fmt.Printf("writeAt: %d\n", w.writeAt)
	reader := model.NewHashReader(w.file.Fd)
	var readAt uint32 = 0
	for {
		var entry *model.Entry
		entry, readAt = w.Read(reader)
		if readAt > 0 {
			fmt.Printf("entry: key:%s val:%s meta:%d exp:%d \n",
				entry.Key, entry.Value, entry.Meta, entry.ExpiresAt)
		} else {
			break
		}
	}
}
