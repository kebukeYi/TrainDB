package lsm

import "github.com/kebukeYi/TrainKV/utils/cache"

type LevelsCache struct {
	indexData *cache.Cache
	blockData *cache.Cache
}

const defaultCacheNums = 1024 * 10

func newLevelsCache(opt *Options) *LevelsCache {
	if opt.CacheNums == 0 {
		opt.CacheNums = defaultCacheNums
	}
	return &LevelsCache{
		indexData: cache.NewCache(opt.CacheNums),
		blockData: cache.NewCache(opt.CacheNums),
	}
}
