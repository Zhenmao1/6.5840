package shardkv

import (
	"time"

	"6.5840/shardctrler"
)

func (kv *ShardKV) CatchConfig() {
	for {
		if kv.killed() {
			return
		}
		time.Sleep(time.Duration(configTime) * time.Millisecond)
		//不是leader，不发起询问
		if _, Isleader := kv.rf.GetState(); !Isleader {
			continue
		}
		//检查是否有shard处于pull或者push状态
		kv.configMu.Lock()
		shardStatus := kv.shardStatus
		kv.configMu.Unlock()
		flag := false
		for i := 0; i < shardctrler.NShards; i++ {
			if (shardStatus[i] == Pulling || shardStatus[i] == Pushing) && !flag {
				flag = true
				DPrintf("kvserver  gid%v me%v shard%d 处于迁移状态%s,不允许查询config", kv.gid, kv.me, i, shardStatus[i])
			}
		}
		if flag {
			continue
		}

		//可以进行配置查询
		config := kv.sm.Query(kv.currentConfig.Num + 1)
		//有配置更新，需要利用raft同步所有的配置
		if config.Num > kv.currentConfig.Num {
			DPrintf("kvserver  gid%v me%v 旧配置%v shard%v，开始发送给其他的同组的server的raft", kv.gid, kv.me, kv.currentConfig, kv.shardStatus)
			op := Op{
				Option: Config,
				Config: config,
				SeqNum: config.Num,
			}
			lastAppliedIndex, _, _ := kv.rf.Start(op)
			waitCh := kv.makeWaitApplyCh(lastAppliedIndex)
			select {
			case replyOp := <-waitCh:
				kv.mu.Lock()
				if ch, ok := kv.waitApplyCh[replyOp.Index]; ok {
					close(ch)
					delete(kv.waitApplyCh, replyOp.Index)
				}
				kv.mu.Unlock()
				//此时已经完成了config的更新
				//立即请求shard，此时lastConfig已经消失，需要补上lastconfig
				//根据last和cur的设置一下pushing和pulling
				//此时收到pushIng的请求返回等待，否则还是返回errwronggroup
				//收到request还要把对应的
			case <-time.After(time.Duration(time.Millisecond * 5000)):
				DPrintf("超时，删除通道")
				kv.mu.Lock()
				if ch, ok := kv.waitApplyCh[lastAppliedIndex]; ok {
					close(ch)
					delete(kv.waitApplyCh, lastAppliedIndex)
				}
				kv.mu.Unlock()
				continue
			}
		}
	}
}

func (kv *ShardKV) RequestShards(args *RequestShardsArgs, reply *RequestShardsReply) {
	kv.configMu.Lock()
	defer kv.configMu.Unlock()
	//配置过期，拒绝迁移数据
	if args.Config.Num < kv.currentConfig.Num {
		reply.Err = ErrConfigNum
		return
	}
	//收到重复的命令
	if kv.completed[int64(args.ClientId)] >= args.SeqNum {
		reply.Err = ErrDulplicate
		return
	}
	//不是leader,拒绝迁移
	if _, Isleader := kv.rf.GetState(); !Isleader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Data = kv.shards[args.ShardIndexes]
	reply.Err = OK
	DPrintf("shardkvgid%v me%v为pulling添加shard%d 内容%v", kv.gid, kv.me, args.ShardIndexes, kv.shards[args.ShardIndexes])
	DPrintf("shardkgid%v me%v收到了pull，发起命令修改对应的shard%v的pushing状态", kv.gid, kv.me, args.ShardIndexes)

	go func(args *RequestShardsArgs) {
		op := Op{
			Option: MoveShard,
			Config: kv.currentConfig,
			//借用seqnum表示是哪一个shard改变了pushing状态
			SeqNum: args.ShardIndexes,
		}
		lastAppliedIndex, _, _ := kv.rf.Start(op)
		waitCh := kv.makeWaitApplyCh(lastAppliedIndex)
		select {
		case replyOp := <-waitCh:
			kv.mu.Lock()
			kv.completed[int64(args.ClientId)] = args.SeqNum
			if ch, ok := kv.waitApplyCh[replyOp.Index]; ok {
				close(ch)
				delete(kv.waitApplyCh, replyOp.Index)
			}
			kv.mu.Unlock()
			return
		case <-time.After(time.Duration(time.Millisecond * 5000)):
			return
		}
		//不再继续请求
	}(args)

}

func (kv *ShardKV) CatchShards() {
	for {
		if kv.killed() {
			return
		}
		if _, Isleader := kv.rf.GetState(); !Isleader {
			time.Sleep(time.Duration(checkShardMovingTime) * time.Millisecond)
			continue
		}
		time.Sleep(time.Duration(checkShardMovingTime) * time.Millisecond)
		for i := 0; i < shardctrler.NShards; i++ {
			if kv.shardStatus[i] == Pulling {
				DPrintf("kvserver gid%v me%v 检查需要pulling shard%v", kv.gid, kv.me, i)
				go kv.SendRequestShardsArgs(i)
				break
			}
		}

	}

}

func (kv *ShardKV) SendRequestShardsArgs(indexes int) {
	args := RequestShardsArgs{
		Config:       kv.currentConfig,
		ShardIndexes: indexes,
		SeqNum:       kv.seqNum + 1,
		ClientId:     kv.gid*10 + kv.me,
	}
	reply := RequestShardsReply{}
	gid := kv.lastConfig.Shards[indexes] // 根据分片找到负责该分片的组
	for {
		if servers, ok := kv.lastConfig.Groups[gid]; ok {
			// 尝试查询分片所在组的每个服务器
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				DPrintf("kvserver gid%v me%v 对 kvserver %v %s发起pulling 指令信息：%v", kv.gid, kv.me, gid, servers[si], args)
				ok := srv.Call("ShardKV.RequestShards", &args, &reply)
				if !ok {
					DPrintf("kvserver gid%v me%v 对 kvserver %v%s发起pulling失败：%v", kv.gid, kv.me, gid, servers[si], args)
				}
				DPrintf("kvserver gid%v me%v 对 kvserver %v%s发起pull返回：%v", kv.gid, kv.me, gid, servers[si], reply)
				if ok && reply.Err == OK {
					kv.shardStatus[args.ShardIndexes] = ErrWaitShard
					kv.seqNum++
					//开启raft推送pulling,要发送最新的config表明这些pulling操作来自最新的配置
					op := Op{
						Option:       AddShard,
						Config:       kv.currentConfig,
						ShardIndexes: indexes,
						Data:         reply.Data,
					}
					lastAppliedIndex, _, _ := kv.rf.Start(op)
					waitCh := kv.makeWaitApplyCh(lastAppliedIndex)
					select {
					case replyOp := <-waitCh:
						kv.mu.Lock()
						if ch, ok := kv.waitApplyCh[replyOp.Index]; ok {
							close(ch)
							delete(kv.waitApplyCh, replyOp.Index)
						}
						kv.mu.Unlock()
						return
					case <-time.After(time.Duration(time.Millisecond * 5000)):
						return
					}
					//不再继续请求
				}

			}
		}

	}

}

func (kv *ShardKV) addShardHandller(op Op) {
	kv.configMu.Lock()
	defer kv.configMu.Unlock()
	//seq字段被借用
	if kv.currentConfig.Num < op.Config.Num {
		return
	}

	kv.shardStatus[op.ShardIndexes] = Servering
	kv.shards[op.ShardIndexes] = op.Data
	DPrintf("kvserver%v %v收到addshard   shard%d %v", kv.gid, kv.me, op.ShardIndexes, op.Data)

}

func (kv *ShardKV) moveShardHandller(op Op) {
	kv.configMu.Lock()
	defer kv.configMu.Unlock()
	//seq字段被借用
	if kv.currentConfig.Num < op.Config.Num {
		return
	}
	kv.shardStatus[op.SeqNum] = NotFind
	DPrintf("kvserver gid%v me%v 收到moveshard   shardstatus%d %v", kv.gid, kv.me, op.SeqNum, kv.shardStatus[op.SeqNum])
}

func (kv *ShardKV) configHandller(op Op) {
	kv.configMu.Lock()
	defer kv.configMu.Unlock()
	if kv.currentConfig.Num >= op.SeqNum {
		return
	}
	DPrintf("kvserver gid%v me%v收到config   旧配置%v shard%v", kv.gid, kv.me, kv.currentConfig, kv.shardStatus)
	kv.lastConfig = kv.currentConfig
	kv.currentConfig = op.Config
	//先设置对应的shard

	//初始配置，不需要请求shard，是这个组就提供，不是就notfind
	if kv.currentConfig.Num == 1 {
		for i := 0; i < shardctrler.NShards; i++ {
			if kv.currentConfig.Shards[i] == kv.gid {
				kv.shardStatus[i] = Servering
			} else {
				kv.shardStatus[i] = NotFind
			}
		}
	} else {
		for i := 0; i < shardctrler.NShards; i++ {
			//kv.askShard[i] = false
			//不是初始配置，上次和这次不一样的shard都需要迁入或者迁出，错误
			//这次归自己，设置为pulling,要去上次所属的group找
			//这次不归自己，直接设置为pushing，拒绝访问即可

			//之前不是我的，现在是我的，pull
			if kv.currentConfig.Shards[i] == kv.gid && kv.lastConfig.Shards[i] != kv.gid {
				kv.shardStatus[i] = Pulling
			} else if kv.currentConfig.Shards[i] != kv.gid && kv.lastConfig.Shards[i] == kv.gid {
				kv.shardStatus[i] = Pushing
			} else {
				continue
			}
		}
	}
	DPrintf("kvserver gid%v me%v收到config  新配置%v shard%v", kv.gid, kv.me, kv.currentConfig, kv.shardStatus)
}
