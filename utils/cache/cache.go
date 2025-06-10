package cache

import (
	"container/list"
	xxhash "github.com/cespare/xxhash/v2"
	"sync"
	"unsafe"
)

type Cache struct {
	m         sync.RWMutex
	wlru      *winLRU
	slru      *segmentedLRU
	door      *BloomFilter
	cmkt      *cmSketch
	total     int32
	threshold int32
	data      map[uint64]*list.Element
}

func NewCache(numEntries int) *Cache {
	const winlruPct = 10
	winlruSz := (winlruPct * numEntries) / 100
	slruSz := int(float64(numEntries) * ((100 - winlruPct) / 100.0))
	slruOne := int(0.2 * float64(slruSz))
	data := make(map[uint64]*list.Element, numEntries)

	return &Cache{
		m:         sync.RWMutex{},
		wlru:      NewWinLRU(winlruSz, data),
		slru:      newSLRU(data, slruOne, slruSz-slruOne),
		door:      newBloomFilter(numEntries, 0.01),
		cmkt:      newCmSketch(int64(numEntries)),
		total:     0,
		threshold: int32(numEntries * 100),
		data:      data,
	}
}

func (c *Cache) Set(key, val interface{}) bool {
	c.m.Lock()
	defer c.m.Unlock()
	return c.set(key, val)
}

func (c *Cache) set(key, val interface{}) bool {
	keyToHash, _ := KeyToHash(key)
	itme := storeItem{
		keyHash: keyToHash,
		value:   val,
		stage:   0,
	}
	eitem, evicted := c.wlru.add(itme)
	if !evicted {
		return true
	}
	// 如果 window 中有被淘汰的数据，会走到这里
	// 需要从 LFU 的 stageOne 部分找到一个淘汰者(未剔除)
	// 二者进行 PK
	victim := c.slru.victim()
	if victim == nil {
		c.slru.add(eitem)
		return true
	}
	// 这里进行 PK，必须在 bloomFilter 中出现过一次，才允许 PK
	// 在 bf 中出现，说明访问频率 >= 2
	if !c.door.Allow(uint32(eitem.keyHash)) {
		return true
	}
	vcount := c.cmkt.Estimate(victim.keyHash)
	icount := c.cmkt.Estimate(eitem.keyHash)
	if vcount > icount {
		return true
	}

	// 执行到这里 说明 winlru的值频率>= slru的值频率; 需要留下winlru的值
	// 留下来的 进入 stageOne, 但是此时 victim 并没有剔除掉, 但是add()方法的逻辑中会进行剔除判断;
	c.slru.add(eitem)
	return true
}

func (c *Cache) Get(key interface{}) (interface{}, bool) {
	c.m.Lock()
	defer c.m.Unlock()
	return c.get(key)
}

func (c *Cache) get(key interface{}) (interface{}, bool) {
	c.total++
	if c.total == c.threshold {
		c.cmkt.Reset()
		c.door.reset()
		c.total = 0
	}

	keyToHash, _ := KeyToHash(key)
	element, ok := c.data[keyToHash]

	if !ok {
		// todo 自动更换热点数据 关键点
		// 不存在也要记录对应的数据频率, 说明是需要下一步进行缓存的;
		// 这样积累的访问次数会 逐渐 替换掉上个阶段内 需要淘汰的`伪高频`数据;
		c.door.Allow(uint32(keyToHash))
		c.cmkt.increment(keyToHash)
		return nil, false
	}
	item := element.Value.(*storeItem)

	c.door.Allow(uint32(keyToHash))
	c.cmkt.increment(item.keyHash)

	value := item.value
	if item.stage == 0 {
		c.wlru.get(element)
	} else {
		c.slru.get(element)
	}

	return value, true
}

func (c *Cache) Del(key interface{}) (interface{}, bool) {
	c.m.Lock()
	defer c.m.Unlock()
	return c.del(key)
}

func (c *Cache) del(key interface{}) (interface{}, bool) {
	keyToHash, _ := KeyToHash(key)
	_, ok := c.data[keyToHash]
	if !ok {
		return 0, false
	}
	//item := element.Value.(*storeItem)
	delete(c.data, keyToHash)
	return keyToHash, true
}

func KeyToHash(key interface{}) (uint64, uint64) {
	if key == nil {
		return 0, 0
	}
	switch k := key.(type) {
	case uint64:
		return k, 0
	case string:
		return MemHashString(k), xxhash.Sum64String(k)
	case []byte:
		return MemHash(k), xxhash.Sum64(k)
	case byte:
		return uint64(k), 0
	case int:
		return uint64(k), 0
	case int32:
		return uint64(k), 0
	case int64:
		return uint64(k), 0
	case uint32:
		return uint64(k), 0
	default:
		panic("#keyToHash(): Key type not supported")
	}
}

type stringStruct struct {
	str unsafe.Pointer
	len int
}

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

func MemHashString(str string) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&str))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

func MemHash(data []byte) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&data))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

func (c *Cache) String() string {
	s := ""
	s += c.wlru.string() + " | " + c.slru.string()
	return s
}
