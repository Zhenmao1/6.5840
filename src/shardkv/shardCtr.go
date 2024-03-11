package shardkv

import "time"

func (kv *ShardKV) CatchConfig() {
	for {
		if kv.killed() {
			return
		}
		time.Sleep(time.Duration(configTime) * time.Millisecond)
		config := kv.sm.Query(-1)
		kv.mu.Lock()
		if config.Num > kv.currentConfig.Num {
			DPrintf("更新配置%v", config)
			kv.lastConfig = kv.currentConfig
			kv.currentConfig = config
		}
		kv.mu.Unlock()
	}
}
