// Raft一致性实现
// Raft服务器(节点管理)
package raft

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"
	"raft/proto/message"
	"sync"
	"time"
)

var ErrorInvalidClient = fmt.Errorf("client was closed")

var ErrorContextCanceled = fmt.Errorf("context canceled")

var ErrorServerAlreadyShutdown = fmt.Errorf("server already shutdown")

// Config 配置
type Config struct {
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
	nodeClients map[string]*grpc.ClientConn

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

	// 退出信号
	quit     chan interface{}
	shutdown bool
	wg       sync.WaitGroup
}

// NewServer 新建一个节点服务
// address为TCP监听的地址,一般给个端口就可以如:8888
// storage为持续化存储数据
// commitChan如果有数据新增,则会通知到这里,业务可以用于监听
// nodeIDs除自己以外的节点列表
func NewServer(address string, storage Storage, commitChan chan<- CommitEntry, config Config, nodeIDs ...string) *Server {
	s := new(Server)
	s.address = address
	s.nodeClients = make(map[string]*grpc.ClientConn)
	for _, v := range nodeIDs {
		if v == address {
			continue
		}
		s.nodeClients[v] = nil
	}
	s.storage = storage
	s.commitChan = commitChan
	s.rpcMsgTimeoutMs = config.rpcMsgTimeoutMs
	s.heartbeatMs = config.heartbeatMs
	s.electionTimeoutMinMs = config.electionTimeoutMinMs
	s.electionTimeoutMaxMs = config.electionTimeoutMaxMs
	s.showLog = config.showLog
	s.autoRedirectMessage = config.autoRedirectMessage
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

// Shutdown 关闭服务
func (s *Server) Shutdown() {
	s.disconnectAll()
	s.raft.stop()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shutdown {
		return
	}
	close(s.quit)
	s.rpcServer.GracefulStop()
	s.wg.Wait()
	s.shutdown = true
}

// listenAndServer 注册RPC、创建raft对象并监听
func (s *Server) listenAndServer() {
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

	for id := range s.nodeClients {
		if s.nodeClients[id] != nil {
			err := s.nodeClients[id].Close()
			s.raft.debugLog("shutdown grpc client: %s, %v", id, err)
			s.nodeClients[id] = nil
		}
	}
}

// listenAddr 获取监听地址
func (s *Server) listenAddr() net.Addr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.listener.Addr()
}

// connectToNode 连接到指定节点
func (s *Server) connect(nodeID string) (*grpc.ClientConn, error) {
	if client := s.nodes()[nodeID]; client != nil {
		return client, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*s.rpcMsgTimeoutMs)
	defer cancel()
	conn, err := grpc.DialContext(ctx, nodeID, grpc.WithInsecure(), grpc.WithBlock())

	s.raft.debugLog("connect to grpc client: %s, %v", nodeID, err)

	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.nodeClients[nodeID] = conn
	s.mu.Unlock()

	return conn, nil
}

// connectToNode 连接到指定节点
func (s *Server) connectToNode(nodeID string, addr net.Addr) error {
	if s.nodes()[nodeID] != nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*s.rpcMsgTimeoutMs)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr.String(), grpc.WithInsecure(), grpc.WithBlock())

	s.raft.debugLog("connect to grpc client: %s, %v", nodeID, err)

	if err != nil {
		return err
	}

	s.mu.Lock()
	s.nodeClients[nodeID] = conn
	s.mu.Unlock()

	return nil
}

// disconnectNode 断开连接
func (s *Server) disconnectNode(nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if client := s.nodeClients[nodeID]; client != nil {
		err := s.nodeClients[nodeID].Close()
		s.raft.debugLog("shutdown grpc client: %s, %v", nodeID, err)
		s.nodeClients[nodeID] = nil
		return err
	}

	return nil
}

// connectToNodes 连接到所有节点
func (s *Server) connectToNodes() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range s.nodeClients {
		ctx, cancel := context.WithTimeout(context.Background(), s.rpcMsgTimeoutMs*time.Millisecond)
		conn, err := grpc.DialContext(ctx, k, grpc.WithInsecure(), grpc.WithBlock())
		if err == nil {
			s.nodeClients[k] = conn
		}
		s.raft.debugLog("connect to grpc client: %s, %v", k, err)
		cancel()
	}
}

// call RPC调用
func (s *Server) call(id string, serviceMethod string, args interface{}, reply interface{}, timeoutMs time.Duration) error {
	if client := s.nodes()[id]; client != nil {
		bg := context.Background()
		ctx, cancel := context.WithTimeout(bg, time.Millisecond*timeoutMs)
		defer cancel()
		return client.Invoke(ctx, serviceMethod, args, reply)
	} else {
		return ErrorInvalidClient
	}
}

// nodes 获取所有节点
func (s *Server) nodes() map[string]*grpc.ClientConn {
	m := make(map[string]*grpc.ClientConn)

	s.mu.RLock()
	defer s.mu.RUnlock()

	for k, v := range s.nodeClients {
		m[k] = v
	}

	return m
}
