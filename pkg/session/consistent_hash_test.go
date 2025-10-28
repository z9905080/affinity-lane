package session

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsistentHash_AddNode(t *testing.T) {
	ch := NewConsistentHash(10)

	ch.AddNode("node1")
	assert.Equal(t, 1, ch.NodeCount())

	ch.AddNode("node2")
	assert.Equal(t, 2, ch.NodeCount())

	// Adding same node should not increase count
	ch.AddNode("node1")
	assert.Equal(t, 2, ch.NodeCount())
}

func TestConsistentHash_RemoveNode(t *testing.T) {
	ch := NewConsistentHash(10)

	ch.AddNode("node1")
	ch.AddNode("node2")
	assert.Equal(t, 2, ch.NodeCount())

	ch.RemoveNode("node1")
	assert.Equal(t, 1, ch.NodeCount())

	// Removing non-existent node should not error
	ch.RemoveNode("node3")
	assert.Equal(t, 1, ch.NodeCount())
}

func TestConsistentHash_GetNode(t *testing.T) {
	ch := NewConsistentHash(150)

	// No nodes
	_, ok := ch.GetNode("key1")
	assert.False(t, ok)

	// Add nodes
	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")

	// Get node for key
	node1, ok := ch.GetNode("session1")
	require.True(t, ok)
	assert.NotEmpty(t, node1)

	// Same key should always return same node
	node2, ok := ch.GetNode("session1")
	require.True(t, ok)
	assert.Equal(t, node1, node2)

	// Different keys may return different nodes
	node3, ok := ch.GetNode("session2")
	require.True(t, ok)
	assert.NotEmpty(t, node3)
}

func TestConsistentHash_Distribution(t *testing.T) {
	ch := NewConsistentHash(150)

	// Add nodes
	numNodes := 10
	for i := 0; i < numNodes; i++ {
		ch.AddNode(fmt.Sprintf("node%d", i))
	}

	// Generate keys and check distribution
	numKeys := 10000
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("session%d", i)
	}

	distribution := ch.GetDistribution(keys)

	// Check that all nodes got some keys
	assert.Equal(t, numNodes, len(distribution))

	// Check that distribution is reasonably balanced
	// Each node should get roughly numKeys/numNodes keys (±30%)
	expectedPerNode := numKeys / numNodes
	for node, count := range distribution {
		t.Logf("Node %s: %d keys (%.2f%%)", node, count, float64(count)/float64(numKeys)*100)

		// Allow 30% deviation from perfect distribution
		minExpected := int(float64(expectedPerNode) * 0.7)
		maxExpected := int(float64(expectedPerNode) * 1.3)

		assert.GreaterOrEqual(t, count, minExpected,
			"Node %s has too few keys: %d (expected >= %d)", node, count, minExpected)
		assert.LessOrEqual(t, count, maxExpected,
			"Node %s has too many keys: %d (expected <= %d)", node, count, maxExpected)
	}
}

func TestConsistentHash_NodeAddRemoveStability(t *testing.T) {
	ch := NewConsistentHash(150)

	// Add initial nodes
	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")

	// Map keys to nodes
	numKeys := 1000
	initialMapping := make(map[string]string)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("session%d", i)
		node, ok := ch.GetNode(key)
		require.True(t, ok)
		initialMapping[key] = node
	}

	// Add a new node
	ch.AddNode("node4")

	// Check how many keys moved
	moved := 0
	for key, oldNode := range initialMapping {
		newNode, ok := ch.GetNode(key)
		require.True(t, ok)
		if oldNode != newNode {
			moved++
		}
	}

	// With consistent hashing, only ~1/4 of keys should move
	// (since we added 1 node to 3 existing nodes)
	movedPercentage := float64(moved) / float64(numKeys) * 100
	t.Logf("Added node: %.2f%% of keys moved", movedPercentage)

	// Should be roughly 25% ± 10%
	assert.Greater(t, movedPercentage, 15.0)
	assert.Less(t, movedPercentage, 35.0)
}

func BenchmarkConsistentHash_GetNode(b *testing.B) {
	ch := NewConsistentHash(150)

	// Add nodes
	for i := 0; i < 10; i++ {
		ch.AddNode(fmt.Sprintf("node%d", i))
	}

	keys := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = fmt.Sprintf("session%d", i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		ch.GetNode(key)
	}
}
