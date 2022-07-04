package kvraft

import (
	"sync"
)

type node struct {
	key   int
	value int
	prev  *node
	next  *node
}

const MAXCNT = 100000

type lru struct {
	head *node
	tail *node
	pos  map[int]*node
	cnt  int
	mu   sync.Mutex
}

func (cache *lru) Init() {
	cache.head = &node{}
	cache.tail = &node{}
	cache.head.prev = nil
	cache.head.next = cache.tail
	cache.tail.prev = cache.head
	cache.tail.next = nil
	cache.pos = make(map[int]*node)
	cache.cnt = 0
}

func (cache *lru) Remove() {
	key := cache.tail.prev.key
	delete(cache.pos, key)
	cache.tail = cache.tail.prev
	cache.cnt--
}

func (cache *lru) Get(key int) (int, bool) {
	tmp, ok := cache.pos[key]
	if ok == false {
		return 0, ok
	}
	tmp.prev.next = tmp.next
	tmp.next.prev = tmp.prev
	tmp.next = cache.head
	tmp.prev = nil
	cache.head.prev = tmp
	cache.head = tmp
	return tmp.value, ok
}

func (cache *lru) Insert(key int, value int) {
	if _, ok := cache.pos[key]; ok {
		cache.pos[key].value = value
		cache.Get(key)
		return
	}
	if cache.cnt == MAXCNT {
		cache.Remove()
	}
	cache.cnt++
	tmp := &node{}
	tmp.key = key
	tmp.value = value
	tmp.next = cache.head
	cache.head.prev = tmp
	tmp.prev = nil
	cache.pos[key] = tmp
}
