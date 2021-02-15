// Raft一致性实现
// Raft服务器(节点管理)
package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"github.com/invxp/raft/proto/message"
	"google.golang.org/grpc"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

var ErrorInvalidClient = fmt.Errorf("invalid rpcClient")

var ErrorContextCanceled = fmt.Errorf("context canceled")

var ErrorServerAlreadyShutdown = fmt.Errorf("server already shutdown")

var ErrorClientAlreadyClosed = fmt.Errorf("rpcClient already closed")

// Config 配置
type Config struct {
	// 选举最短超时时间
	ElectionTimeoutMinMs int
	// 选举最长超时时间
	ElectionTimeoutMaxMs int
	// Leader心跳间隔
	HeartbeatMs time.Duration
	// rpc请求超时时间
	RPCMsgTimeoutMs time.Duration
	// 是否打印日志
	ShowLog bool
	// 是否自动转发消息(如果在Follower上提交日志,会自动转发到Leader)
	AutoRedirectMessage bool
	// CMD-Server的监听地址
	CMDServerAddress string
	// CMD-Server的密钥
	CMDServerAuth string
}

// rpcClient 客户端结构体
type rpcClient struct {
	*grpc.ClientConn
	isClose     bool
	isConnected bool
}

type Server struct {
	// 互斥锁
	mu sync.RWMutex

	// server 自身地址
	address string

	// raft 对象
	raft *raft

	// 持久化存储器
	storage Storage

	// RPC服务器代理
	rpcProxy *rpcProxy

	// RPC服务器
	rpcServer *grpc.Server

	// 监听器(TCP)
	listener net.Listener

	// 数据回调通道
	commitChan chan<- CommitEntry

	// 客户端列表(除自己以外)
	nodeClients map[string]*rpcClient

	// 选举最短超时时间
	electionTimeoutMinMs int
	// 选举最长超时时间
	electionTimeoutMaxMs int
	// Leader心跳间隔
	heartbeatMs time.Duration
	// rpc请求超时时间
	rpcMsgTimeoutMs time.Duration
	// 是否打印日志
	showLog bool
	// 是否自动转发消息(如果在Follower上提交日志,会自动转发到Leader)
	autoRedirectMessage bool

	// 运行时间
	runTime time.Time

	// 退出信号
	quit     chan interface{}
	shutdown bool
	wg       sync.WaitGroup

	// CMD服务器
	cmd *cmdServer
}

// NewServer 新建一个raft服务
// address为TCP监听的地址,一般给个端口就可以如:8888
// commitChan如果有数据新增,则会通知到这里,业务可以用于监听
// config配置信息,需要确保electionMin比Max要小,并且heartbeat也要比electionMin小这三个参数才可生效
// 可根据实际情况调整时间,默认min(150ms), max(300ms), heartbeat(20ms)
// nodeIDs除自己以外的节点列表(新的节点把老的节点的地址都填上即可相互连接)
func NewServer(address string, commitChan chan<- CommitEntry, config *Config, nodeIDs ...string) *Server {
	s := new(Server)
	s.address = address
	s.nodeClients = make(map[string]*rpcClient)
	s.shutdown = true

	var lastNodes []string
	if b, err := ioutil.ReadFile("node"); err == nil {
		d := gob.NewDecoder(bytes.NewBuffer(b))
		if err := d.Decode(&lastNodes); err != nil {
			_ = os.Remove("node")
			log.Fatal(err)
		}
	}

	nodeIDs = append(nodeIDs, lastNodes...)

	for _, v := range nodeIDs {
		if v == address {
			continue
		}
		s.nodeClients[v] = &rpcClient{}
	}
	s.storage = NewMapStorage()

	s.commitChan = commitChan
	s.rpcMsgTimeoutMs = 500
	s.heartbeatMs = 20
	s.electionTimeoutMinMs = 150
	s.electionTimeoutMaxMs = 300
	s.showLog = true
	s.autoRedirectMessage = true

	if config != nil {
		s.showLog = config.ShowLog
		s.autoRedirectMessage = config.AutoRedirectMessage
		if config.ElectionTimeoutMinMs > 0 && config.ElectionTimeoutMaxMs > config.ElectionTimeoutMinMs && config.HeartbeatMs > 0 && config.HeartbeatMs < time.Duration(config.ElectionTimeoutMinMs) {
			s.electionTimeoutMinMs = config.ElectionTimeoutMinMs
			s.electionTimeoutMaxMs = config.ElectionTimeoutMaxMs
			s.heartbeatMs = config.HeartbeatMs
		}
		if config.RPCMsgTimeoutMs > 0 {
			s.rpcMsgTimeoutMs = config.RPCMsgTimeoutMs
		}
		if config.CMDServerAddress != "" {
			s.cmd = &cmdServer{srv: s}
			s.cmd.serv(config.CMDServerAddress, config.CMDServerAuth)
		}
	}

	return s
}

// Server 开启服务
func (s *Server) Server() {
	s.listenAndServer()
	s.connectToNodes()
	s.raft.run()
}

// Commit 提交日志
func (s *Server) Commit(command string) bool {
	return s.raft.submitData(command)
}

// Status 获取状态
func (s *Server) Status() (int32, bool, string) {
	return s.raft.status()
}

// Logs 获取日志
// n:0(全部)
// n>0(正数n个)
// n<0(倒数n个)
func (s *Server) Logs(n int) []LogEntry {
	s.raft.mu.RLock()
	defer s.raft.mu.RUnlock()

	l := make([]LogEntry, 0)

	if n >= 0 {
		count := 0
		for _, val := range s.raft.log {
			l = append(l, LogEntry{val.Command, val.Term})
			count++
			if n > 0 && count >= n {
				break
			}
		}
	} else {
		count := -n
		for i := len(s.raft.log) - 1; i >= 0; i-- {
			l = append(l, LogEntry{s.raft.log[i].Command, s.raft.log[i].Term})
			count--
			if count <= 0 {
				break
			}
		}
	}

	return l
}

// Nodes 获取所有节点
// alive 只返回活着的节点
func (s *Server) Nodes(alive ...bool) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	node := make([]string, 0)

	for key, val := range s.nodeClients {
		if len(alive) > 0 && alive[0] == true {
			if val.isConnected && !val.isClose {
				node = append(node, key)
			}
		} else {
			node = append(node, key)
		}
	}

	node = append(node, s.address)

	return node
}

// Shutdown 关闭服务
func (s *Server) Shutdown() {
	if s.shutdown {
		return
	}

	s.disconnectAll()
	s.raft.stop()
	s.mu.Lock()
	defer s.mu.Unlock()
	close(s.quit)
	s.rpcServer.GracefulStop()
	s.wg.Wait()

	_ = s.cmd.shutdown()

	s.shutdown = true
}

// listenAndServer 注册RPC、创建raft对象并监听
func (s *Server) listenAndServer() {
	if !s.shutdown {
		return
	}
	var err error
	s.listener, err = net.Listen("tcp", s.address)
	if err != nil {
		log.Fatal(err)
	}
	s.raft = create(s.address, s, s.storage, s.commitChan, s.rpcMsgTimeoutMs, s.heartbeatMs, s.electionTimeoutMinMs, s.electionTimeoutMaxMs, s.showLog, s.autoRedirectMessage, s.nodeClients)
	s.rpcServer = grpc.NewServer()
	s.rpcProxy = &rpcProxy{raft: s.raft}
	s.shutdown = false
	s.quit = make(chan interface{})
	s.runTime = time.Now()

	message.RegisterMessageServer(s.rpcServer, s.rpcProxy)

	s.raft.debugLog("grpc server started...")

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			err := s.rpcServer.Serve(s.listener)
			if err != nil {
				select {
				case <-s.quit:
					s.raft.debugLog("grpc server close...")
					return
				default:
					log.Fatal("accept error:", err)
				}
			}
		}
	}()
}

// disconnectAll 断开所有当前连接的节点
func (s *Server) disconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id, val := range s.nodeClients {
		if !val.isConnected || val.isClose || val.ClientConn == nil {
			continue
		}
		err := s.nodeClients[id].Close()
		s.raft.debugLog("shutdown grpc rpcClient: %s, %v", id, err)
		s.nodeClients[id].isClose = true
		s.nodeClients[id].isConnected = false
		s.nodeClients[id].ClientConn = nil
	}
}

// connectToNode 连接到指定节点
func (s *Server) connect(nodeID string, force bool) (*grpc.ClientConn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if client := s.nodeClients[nodeID]; client != nil {
		if client.isClose && !force {
			return nil, ErrorClientAlreadyClosed
		}
		if client.isConnected {
			return client.ClientConn, nil
		}
	} else {
		s.nodeClients[nodeID] = &rpcClient{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*s.rpcMsgTimeoutMs)
	defer cancel()
	conn, err := grpc.DialContext(ctx, nodeID, grpc.WithInsecure(), grpc.WithBlock())

	s.raft.debugLog("connect to grpc rpcClient: %s, %v", nodeID, err)

	if err != nil {
		return nil, err
	}

	s.nodeClients[nodeID].ClientConn = conn
	s.nodeClients[nodeID].isConnected = true
	s.nodeClients[nodeID].isClose = false

	return conn, nil
}

// disconnectNode 断开连接
func (s *Server) disconnectNode(nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if client := s.nodeClients[nodeID]; client != nil {
		if !client.isConnected || client.isClose || client.ClientConn == nil {
			return nil
		}
		err := s.nodeClients[nodeID].Close()
		s.raft.debugLog("shutdown grpc rpcClient: %s, %v", nodeID, err)
		s.nodeClients[nodeID].isClose = true
		s.nodeClients[nodeID].isConnected = false
		s.nodeClients[nodeID].ClientConn = nil
		return err
	}

	return nil
}

// connectToNodes 连接到所有节点
func (s *Server) connectToNodes() {
	wg := &sync.WaitGroup{}

	for k, v := range s.nodes() {
		wg.Add(1)
		go func(node string, client *rpcClient) {
			defer wg.Done()
			if client.isClose || client.isConnected {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), s.rpcMsgTimeoutMs*time.Millisecond)
			conn, err := grpc.DialContext(ctx, node, grpc.WithInsecure(), grpc.WithBlock())
			if err == nil {
				s.mu.Lock()
				if s.nodeClients[node] == nil {
					s.nodeClients[node] = &rpcClient{}
				}
				s.nodeClients[node].ClientConn = conn
				s.nodeClients[node].isConnected = true
				s.nodeClients[node].isClose = false
				s.mu.Unlock()
			}
			s.raft.debugLog("connect to grpc rpcClient: %s, %v", node, err)
			cancel()
		}(k, v)
	}

	wg.Wait()
}

// call RPC调用
func (s *Server) call(id string, serviceMethod string, args interface{}, reply interface{}, timeoutMs time.Duration) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if client := s.nodeClients[id]; client != nil {
		if client.isClose {
			return ErrorClientAlreadyClosed
		}
		if !client.isConnected {
			go func() {
				_, _ = s.connect(id, false)
			}()
			return nil
		}
		bg := context.Background()
		ctx, cancel := context.WithTimeout(bg, time.Millisecond*timeoutMs)
		defer cancel()
		return client.Invoke(ctx, serviceMethod, args, reply)
	} else {
		return ErrorInvalidClient
	}
}

// nodes 获取所有节点
func (s *Server) nodes() map[string]*rpcClient {
	m := make(map[string]*rpcClient)

	s.mu.RLock()
	defer s.mu.RUnlock()

	for k, v := range s.nodeClients {
		m[k] = v
	}

	return m
}
