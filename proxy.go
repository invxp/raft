package raft

import (
	"context"
	"raft/proto/message"
)

type rpcProxy struct {
	message.MessageServer
	raft *raft
}

// RequestVote 选举
func (p *rpcProxy) RequestVote(ctx context.Context, in *message.RequestVoteRequest) (*message.RequestVoteReply, error) {
	select {
	case <-ctx.Done():
		return nil, ErrorContextCanceled
	default:
		return p.raft.RequestVote(in)
	}
}

// AppendEntries 日志复制
func (p *rpcProxy) AppendEntries(ctx context.Context, in *message.AppendEntriesRequest) (*message.AppendEntriesReply, error) {
	select {
	case <-ctx.Done():
		return nil, ErrorContextCanceled
	default:
		return p.raft.AppendEntries(in)
	}
}

// ForwardMessage 数据转发
func (p *rpcProxy) ForwardMessage(ctx context.Context, in *message.ForwardMessageRequest) (*message.ForwardMessageReply, error) {
	select {
	case <-ctx.Done():
		return nil, ErrorContextCanceled
	default:
		return p.raft.ForwardMessage(in)
	}
}

// Replica 节点扩缩容
func (p *rpcProxy) Replica(ctx context.Context, in *message.ReplicaRequest) (*message.ReplicaReply, error) {
	select {
	case <-ctx.Done():
		return nil, ErrorContextCanceled
	default:
		return p.raft.Replica(in)
	}
}
