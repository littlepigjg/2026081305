package idgen

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

type Generator struct {
	mu          sync.Mutex
	counter     uint64
	nodeID      string
	prefix      string
	useTimestamp bool
}

var (
	globalGen     *Generator
	globalOnce    sync.Once
)

func init() {
	globalGen = NewGenerator("scheduler", true)
}

func NewGenerator(prefix string, useTimestamp bool) *Generator {
	return &Generator{
		prefix:       prefix,
		useTimestamp: useTimestamp,
		nodeID:       generateNodeID(),
	}
}

func Global() *Generator {
	globalOnce.Do(func() {
		if globalGen == nil {
			globalGen = NewGenerator("scheduler", true)
		}
	})
	return globalGen
}

func generateNodeID() string {
	b := make([]byte, 3)
	_, err := rand.Read(b)
	if err != nil {
		return "000000"
	}
	return hex.EncodeToString(b)
}

func (g *Generator) SetPrefix(p string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.prefix = p
}

func (g *Generator) Next() string {
	g.mu.Lock()
	defer g.mu.Unlock()

	atomic.AddUint64(&g.counter, 1)
	cnt := atomic.LoadUint64(&g.counter)

	var id string
	if g.useTimestamp {
		ts := time.Now().UnixNano()
		id = fmt.Sprintf("%s-%s-%x-%d-%d", g.prefix, g.nodeID, ts, cnt, time.Now().UnixMilli()%1000)
	} else {
		id = fmt.Sprintf("%s-%s-%d-%d", g.prefix, g.nodeID, cnt, time.Now().UnixMilli()%1000)
	}
	return id
}

func (g *Generator) NextShort() string {
	g.mu.Lock()
	defer g.mu.Unlock()

	atomic.AddUint64(&g.counter, 1)
	cnt := atomic.LoadUint64(&g.counter)

	h := fnv.New32a()
	ts := fmt.Sprintf("%d-%d-%d", time.Now().UnixNano(), cnt, len(g.prefix))
	h.Write([]byte(ts))
	sum := h.Sum(nil)

	return hex.EncodeToString(sum)[:12]
}

func (g *Generator) NextWithPrefix(prefix string) string {
	g.mu.Lock()
	defer g.mu.Unlock()

	atomic.AddUint64(&g.counter, 1)
	cnt := atomic.LoadUint64(&g.counter)

	ts := time.Now().UnixNano()
	return fmt.Sprintf("%s-%s-%x-%d", prefix, g.nodeID, ts, cnt)
}

func Next() string             { return Global().Next() }
func NextShort() string        { return Global().NextShort() }
func NextWithPrefix(p string) string  { return Global().NextWithPrefix(p) }

func ValidateID(id string) bool {
	if len(id) == 0 || len(id) > 256 {
		return false
	}
	for i, c := range id {
		if c < 32 || c > 126 {
			return false
		}
		if i == 0 && c == '-' {
			return false
		}
		if i == len(id)-1 && c == '-' {
			return false
		}
	}
	return true
}

func IsIDUnique(id string, existing map[string]bool) bool {
	return !existing[id]
}
