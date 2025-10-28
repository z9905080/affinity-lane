package session

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// ConsistentHash implements consistent hashing with virtual nodes
type ConsistentHash struct {
	mu           sync.RWMutex
	hashRing     []uint32          // Sorted hash values
	hashMap      map[uint32]string // Hash to node ID mapping
	nodes        map[string]bool   // Active nodes
	virtualNodes int               // Number of virtual nodes per physical node
}

// NewConsistentHash creates a new consistent hash instance
func NewConsistentHash(virtualNodes int) *ConsistentHash {
	if virtualNodes <= 0 {
		virtualNodes = 150 // default
	}
	return &ConsistentHash{
		hashMap:      make(map[uint32]string),
		nodes:        make(map[string]bool),
		virtualNodes: virtualNodes,
	}
}

// AddNode adds a node to the hash ring
func (ch *ConsistentHash) AddNode(nodeID string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.nodes[nodeID] {
		return // node already exists
	}

	ch.nodes[nodeID] = true

	// Add virtual nodes
	for i := 0; i < ch.virtualNodes; i++ {
		virtualKey := nodeID + "#" + strconv.Itoa(i)
		hash := ch.hash(virtualKey)
		ch.hashRing = append(ch.hashRing, hash)
		ch.hashMap[hash] = nodeID
	}

	// Sort the hash ring
	sort.Slice(ch.hashRing, func(i, j int) bool {
		return ch.hashRing[i] < ch.hashRing[j]
	})
}

// RemoveNode removes a node from the hash ring
func (ch *ConsistentHash) RemoveNode(nodeID string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if !ch.nodes[nodeID] {
		return // node doesn't exist
	}

	delete(ch.nodes, nodeID)

	// Remove virtual nodes
	newRing := make([]uint32, 0, len(ch.hashRing)-ch.virtualNodes)
	for _, hash := range ch.hashRing {
		if ch.hashMap[hash] != nodeID {
			newRing = append(newRing, hash)
		} else {
			delete(ch.hashMap, hash)
		}
	}
	ch.hashRing = newRing
}

// GetNode returns the node for a given key
func (ch *ConsistentHash) GetNode(key string) (string, bool) {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.hashRing) == 0 {
		return "", false
	}

	hash := ch.hash(key)

	// Binary search for the first node with hash >= key hash
	idx := sort.Search(len(ch.hashRing), func(i int) bool {
		return ch.hashRing[i] >= hash
	})

	// Wrap around if we've gone past the end
	if idx == len(ch.hashRing) {
		idx = 0
	}

	nodeID := ch.hashMap[ch.hashRing[idx]]
	return nodeID, true
}

// GetNodes returns all active node IDs
func (ch *ConsistentHash) GetNodes() []string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	nodes := make([]string, 0, len(ch.nodes))
	for nodeID := range ch.nodes {
		nodes = append(nodes, nodeID)
	}
	return nodes
}

// NodeCount returns the number of active nodes
func (ch *ConsistentHash) NodeCount() int {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	return len(ch.nodes)
}

// GetDistribution returns the distribution of keys across nodes for analysis
func (ch *ConsistentHash) GetDistribution(keys []string) map[string]int {
	distribution := make(map[string]int)
	for _, key := range keys {
		if node, ok := ch.GetNode(key); ok {
			distribution[node]++
		}
	}
	return distribution
}

// hash computes the hash value for a key using CRC32
func (ch *ConsistentHash) hash(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}
