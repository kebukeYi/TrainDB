package lsm

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/kebukeYi/TrainDB/common"
	"github.com/kebukeYi/TrainDB/file"
	"github.com/kebukeYi/TrainDB/model"
	"github.com/kebukeYi/TrainDB/pb"
	"github.com/kebukeYi/TrainDB/utils"
	"github.com/pkg/errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

type SSTable struct {
	mux            *sync.RWMutex
	fid            uint64
	file           *file.MmapFile
	maxKey         []byte
	minKey         []byte
	tableIndex     *pb.TableIndex
	hasBloomFilter bool
	idxOffset      int
	idxLen         int
	creationTime   time.Time
}

func OpenSStable(opt *utils.FileOptions) *SSTable {
	mmapFile, err := file.OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	if err != nil {
		return nil
	}
	return &SSTable{file: mmapFile, fid: opt.FID, mux: &sync.RWMutex{}}
}

func (sst *SSTable) Init() error {
	var ko *pb.BlockOffset
	var err error
	if ko, err = sst.initTable(); err != nil {
		return err
	}
	stat, _ := sst.file.Fd.Stat()
	statType := stat.Sys().(*syscall.Stat_t)
	sst.creationTime = time.Unix(statType.Ctim.Sec, statType.Ctim.Nsec)
	keyBytes := ko.GetKey()
	minKey := make([]byte, len(keyBytes))
	copy(minKey, keyBytes)
	sst.minKey = minKey
	sst.maxKey = minKey
	return nil
}

func (sst *SSTable) initTable() (firstBlock *pb.BlockOffset, err error) {
	readPos := len(sst.file.Buf)

	readPos -= 4
	checkSumLenArr := sst.readCheckError(readPos, 4)
	checksumLen := int(model.BytesToU32(checkSumLenArr))
	if checksumLen < 0 {
		return nil, errors.New("checksum length less than zero. Data corrupted")
	}

	readPos -= checksumLen
	expectedChk := sst.readCheckError(readPos, checksumLen)

	readPos -= 4
	buf := sst.readCheckError(readPos, 4)
	sst.idxLen = int(model.BytesToU32(buf))

	readPos -= sst.idxLen
	sst.idxOffset = readPos
	indexData := sst.readCheckError(readPos, sst.idxLen)

	if err := utils.VerifyChecksum(indexData, expectedChk); err != nil {
		return nil, errors.Wrapf(err, "failed to verify checksum for table: %s", sst.file.Fd.Name())
	}
	indexTable := &pb.TableIndex{}
	if err := proto.UnmarshalMerge(indexData, indexTable); err != nil {
		return nil, err
	}
	sst.tableIndex = indexTable
	sst.hasBloomFilter = len(indexTable.BloomFilter) > 0
	if len(indexTable.GetOffsets()) > 0 {
		return indexTable.GetOffsets()[0], nil
	}
	return nil, errors.New("ssTable read indexTable fail, data offset[] is nil.")
}

func (sst *SSTable) Indexs() *pb.TableIndex {
	return sst.tableIndex
}

// SetMaxKey max 需要使用table的迭代器,来获取最后一个block的最后一个key;
func (sst *SSTable) SetMaxKey(maxKey []byte) {
	sst.maxKey = maxKey
}

func (sst *SSTable) MaxKey() []byte {
	return sst.maxKey
}

func (sst *SSTable) MinKey() []byte {
	return sst.minKey
}

func (sst *SSTable) FID() uint64 {
	return sst.fid
}

func (sst *SSTable) read(off, size int) ([]byte, error) {
	if len(sst.file.Buf) > 0 {
		if len(sst.file.Buf[off:]) < size {
			return nil, io.EOF
		}
	}
	res := make([]byte, size)
	_, err := sst.file.Read(res, int64(off))
	return res, err
}

func (sst *SSTable) readCheckError(off, sz int) []byte {
	buf, err := sst.read(off, sz)
	common.Panic(err)
	return buf
}

func (sst *SSTable) Bytes(off, sz int) ([]byte, error) {
	return sst.file.Bytes(off, sz)
}

// Size 返回底层文件的尺寸;
func (sst *SSTable) Size() int64 {
	fileStats, err := sst.file.Fd.Stat()
	common.Panic(err)
	return fileStats.Size()
}

func (sst *SSTable) GetCreatedAt() *time.Time {
	return &sst.creationTime
}

func (sst *SSTable) SetCreatedAt(t *time.Time) {
	sst.creationTime = *t
}

func (sst *SSTable) HasBloomFilter() bool {
	return sst.hasBloomFilter
}

func (sst *SSTable) Detele() error {
	return sst.file.Delete()
}
func (sst *SSTable) Truncature(size int64) error {
	return sst.file.Truncature(size)
}

func (sst *SSTable) Close() error {
	return sst.file.Close()
}

func GetSSTablePathFromId(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d.sst", id))
}
