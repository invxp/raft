# Raft

Raft 共识算法基于Go实现, 能力列表:
1. Leader Election √
2. Log Replication √
3. Log Recover √
4. Log Compaction ❌
5. Membership Management √

Raft介绍:

* https://www.jianshu.com/p/5aed73b288f7
* https://github.com/baidu/braft/blob/master/docs/cn/raft_protocol.md
* https://github.com/maemual/raft-zh_cn/blob/master/raft-zh_cn.md

实现参考:

* https://eli.thegreenplace.net
* https://github.com/eliben/raft

## 如何使用

可以直接阅读源码来学习具体的实现, 如果实在懒得看, 可以按照下面做:

```go
package main

func TestRaft(t *testing.T) {
    //具体可参考TestMainFunctions
    //创建日志回调记录
    commit := make(chan raft.CommitEntry)
    //创建默认配置信息
    config := raft.Config{150, 300, 50, 3000, true, true}
    
    //开始监听所有服务
    server := raft.NewServer(":0", raft.NewMapStorage(), commit, config)
    server.Server()
    
    exit := make(chan interface{})
    go func() {
        for {
            select {
            case c := <-commit:
                //如果有数据提交会通知
                log.Println(c)
            case <-exit:
                return
            }
        }
    }()
    
    //提交一条日志(如果只有一个节点会提交失败)
    server.Commit("TEST-LOG")
    
    time.Sleep(time.Second * 10)
    
    //退出
    server.Shutdown()
    close(exit)
}
```

测试用例可以这样做:

```
$ go test -v -race -run @XXXXX(具体方法名)
PASS / FAILED
```

或测试全部用例:
```
$ go test -v -race
```

## TODO
1. Log Compaction(日志快照与压缩)
2. 缩容场景处理
