package raft

import (
	"github.com/fortytw2/leaktest"
	"log"
	"testing"
	"time"
)

func TestRaft(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	//具体可参考TestMainFunctions
	//创建日志回调记录
	commit := make(chan CommitEntry)
	//创建默认配置信息
	config := &Config{150, 300, 50, 3000, true, true}

	//开始监听所有服务
	server := NewServer(":0", commit, config)
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

	//提交一条日志
	server.Commit("TEST-LOG")

	//等待数据同步
	sleepMs(100)

	//退出
	server.Shutdown()
	close(exit)
}

func TestMainFunctions(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	//添加三个本地节点(实际线上生产过程中,必然是一个一个的启动)
	var nodes []string
	nodes = append(nodes, "localhost:1111")
	nodes = append(nodes, "localhost:2222")
	nodes = append(nodes, "localhost:3333")

	servers := make(map[string]*Server)
	servers["localhost:1111"] = nil
	servers["localhost:2222"] = nil
	servers["localhost:3333"] = nil

	exit1 := make(chan struct{})
	exit2 := make(chan struct{})
	exit3 := make(chan struct{})

	{
		commit := make(chan CommitEntry)
		//创建默认配置信息
		config := &Config{150, 300, 50, 3000, true, true}
		//开始监听所有服务
		//启动第一个服务,因为是第一个,所以没有任何节点信息
		servers["localhost:1111"] = NewServer("localhost:1111", commit, config)
		servers["localhost:1111"].Server()

		go func(close chan struct{}) {
			for {
				select {
				case c := <-commit:
					log.Println("client: 1111 got data: ", c)
				case _ = <-close:
					log.Println("client: 1111 close")
					return
				default:
				}
			}
		}(exit1)
	}

	time.Sleep(time.Second)

	{
		commit := make(chan CommitEntry)
		//创建默认配置信息
		config := Config{150, 300, 50, 3000, true, true}
		//开始监听所有服务,因为先前启动了1111，所以nodes里面要把1111加上
		servers["localhost:2222"] = NewServer("localhost:2222", commit, &config, "localhost:1111")
		servers["localhost:2222"].Server()

		go func(close chan struct{}) {
			for {
				select {
				case c := <-commit:
					log.Println("client: 2222 got data: ", c)
				case _ = <-close:
					log.Println("client: 2222 close")
					return
				default:
				}
			}
		}(exit2)
	}

	time.Sleep(time.Second)

	servers["localhost:2222"].Commit("2 is leader")

	time.Sleep(time.Second)

	{
		commit := make(chan CommitEntry)
		//创建默认配置信息
		config := Config{150, 300, 50, 3000, true, true}
		//开始监听所有服务
		servers["localhost:3333"] = NewServer("localhost:3333", commit, &config, "localhost:1111", "localhost:2222")
		servers["localhost:3333"].Server()
		go func(close chan struct{}) {
			for {
				select {
				case c := <-commit:
					log.Println("client: 3333 got data: ", c)
				case _ = <-close:
					log.Println("client: 3333 close")
					return
				default:
				}
			}
		}(exit3)
	}

	time.Sleep(time.Second)

	servers["localhost:1111"].Commit("3 added to node")

	time.Sleep(time.Second)

	servers["localhost:1111"].Shutdown()

	servers["localhost:2222"].Shutdown()

	time.Sleep(time.Second)

	servers["localhost:1111"].Server()

	servers["localhost:2222"].Server()

	time.Sleep(time.Second * 3)

	servers["localhost:3333"].Commit("test logs")

	time.Sleep(time.Second * 2)

	servers["localhost:3333"].Shutdown()

	time.Sleep(time.Second * 2)

	servers["localhost:2222"].Commit("3333 shutdown")

	time.Sleep(time.Second * 2)

	servers["localhost:3333"].Server()

	time.Sleep(time.Second * 2)

	servers["localhost:3333"].Commit("3333 resume")

	time.Sleep(time.Second * 2)

	servers["localhost:1111"].Shutdown()
	close(exit1)

	servers["localhost:2222"].Shutdown()
	close(exit2)

	servers["localhost:3333"].Shutdown()
	close(exit3)
}
