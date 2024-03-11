package shardctrler

import (
	"fmt"
	"hash/crc32"
	"sort"
)

func hashFunc(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// ring算法
func (sc *ShardCtrler) rebalanceShards() {
	for i := 0; i < NShards; i++ {
		shardHash := hashFunc(string(rune('a' + i))) // 为简化起见，假设分片ID的字符表示（'a' + i
		idx := sort.Search(len(sc.sortedHashes), func(j int) bool {
			return sc.sortedHashes[j] >= shardHash
		})
		if len(sc.sortedHashes) == 0 {
			sc.configs[len(sc.configs)-1].Groups = map[int][]string{}
			for i := 0; i < NShards; i++ {
				sc.configs[len(sc.configs)-1].Shards[i] = 0
			}
			return
		}
		if idx == len(sc.sortedHashes) {
			idx = 0 // 如果没有找到更大的哈希值，则环绕到列表开始
		}
		gid := sc.ring[sc.sortedHashes[idx]]
		sc.configs[len(sc.configs)-1].Shards[i] = gid
	}
}

const VirtualNodes = 10 // 为每个组定义虚拟节点的数量

func (sc *ShardCtrler) updateRing() {
	config := sc.configs[len(sc.configs)-1]
	sc.ring = make(map[uint32]int)
	sc.sortedHashes = nil
	for gid, _ := range config.Groups {
		for v := 0; v < VirtualNodes; v++ {
			virtualNodeKey := fmt.Sprintf("%d-%d", gid, v)
			hash := hashFunc(virtualNodeKey)
			sc.ring[hash] = gid
			sc.sortedHashes = append(sc.sortedHashes, hash)
		}
	}
	sort.Slice(sc.sortedHashes, func(i, j int) bool { return sc.sortedHashes[i] < sc.sortedHashes[j] })
}
