package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"raft/transport/pb"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"github.com/samber/lo"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Map[K comparable, V any] struct {
	m  *sync.RWMutex
	kv map[K]V
}

func NewMap[K comparable, V any]() *Map[K, V] {
	return &Map[K, V]{
		m:  &sync.RWMutex{},
		kv: make(map[K]V),
	}
}

func (s *Map[K, V]) Get(k K) (V, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	v, ok := s.kv[k]
	if ok {
		return v, nil
	}

	return v, fmt.Errorf("not found key %s", k)
}

func (s *Map[K, V]) Set(k K, v V) error {
	s.m.Lock()
	defer s.m.Unlock()

	_, ok := s.kv[k]
	if !ok {
		s.kv[k] = v
		return nil
	}

	return fmt.Errorf("key %s already exist", k)
}

func (s *Map[K, V]) Del(k K) (V, error) {
	s.m.Lock()
	defer s.m.Unlock()

	v, ok := s.kv[k]
	if ok {
		delete(s.kv, k)
		return v, nil
	}

	return v, fmt.Errorf("not found key %s", k)
}

func (s *Map[K, V]) GetOrNew(k K, h func() (V, error)) (V, error) {
	s.m.Lock()
	defer s.m.Unlock()

	v, ok := s.kv[k]
	if ok {
		return v, nil
	}

	n, err := h()
	if err != nil {
		return n, err
	}

	s.kv[k] = n
	return n, nil
}

func (s *Map[K, V]) Marshal() ([]byte, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	return json.Marshal(s.kv)
}

var _ raft.AppendFuture = &AppendFuture{}

type PeerContainer struct {
	ServerID      raft.ServerID
	ServerAddress raft.ServerAddress
}

type AppendFuture struct {
	once  *sync.Once
	done  chan struct{}
	error error

	requestID string
	start     time.Time
	request   *raft.AppendEntriesRequest
	response  *raft.AppendEntriesResponse
}

func (t *AppendFuture) Error() error {
	<-t.done
	return t.error
}

func (t *AppendFuture) Start() time.Time {
	return t.start
}

func (t *AppendFuture) Request() *raft.AppendEntriesRequest {
	return t.request
}

func (t *AppendFuture) Response() *raft.AppendEntriesResponse {
	return t.response
}

var _ raft.AppendPipeline = &AppendPipeline{}

type AppendPipeline struct {
	once     *sync.Once
	reply    chan raft.AppendFuture
	inflight chan *AppendFuture
	stream   pb.TransportService_AppendEntriesPipelineClient
}

func (t *AppendPipeline) process() {
	for appendFuture := range t.inflight {
		response, err := t.stream.Recv()
		if err != nil {
			appendFuture.error = err
		} else {
			if response.Error != "" {
				appendFuture.error = fmt.Errorf(response.Error)
			} else {
				appendFuture.response = &raft.AppendEntriesResponse{
					RPCHeader: raft.RPCHeader{
						ProtocolVersion: raft.ProtocolVersion(response.AppendEntriesResponse.RPCHeader.ProtocolVersion),
						ID:              response.AppendEntriesResponse.RPCHeader.ID,
						Addr:            response.AppendEntriesResponse.RPCHeader.Addr,
					},
					Term:           response.AppendEntriesResponse.Term,
					LastLog:        response.AppendEntriesResponse.LastLog,
					Success:        response.AppendEntriesResponse.Success,
					NoRetryBackoff: response.AppendEntriesResponse.NoRetryBackoff,
				}
			}
		}
		appendFuture.once.Do(func() {
			close(appendFuture.done)
		})
		select {
		case <-t.stream.Context().Done():
		case t.reply <- appendFuture:
		}
	}
}

func (t *AppendPipeline) AppendEntries(request *raft.AppendEntriesRequest, response *raft.AppendEntriesResponse) (raft.AppendFuture, error) {
	appendFuture := &AppendFuture{
		once:      &sync.Once{},
		done:      make(chan struct{}, 1),
		error:     nil,
		requestID: uuid.New().String(),
		start:     time.Now(),
		request:   request,
		response:  response,
	}

	err := t.stream.Send(&pb.AppendEntriesPipelineRequest{
		RequestID: appendFuture.requestID,
		AppendEntriesRequest: &pb.AppendEntriesRequest{
			RPCHeader: &pb.RPCHeader{
				ProtocolVersion: pb.ProtocolVersion(appendFuture.Request().ProtocolVersion),
				ID:              appendFuture.request.ID,
				Addr:            appendFuture.request.Addr,
			},
			Term:         appendFuture.request.Term,
			Leader:       appendFuture.request.Leader,
			PrevLogEntry: appendFuture.request.PrevLogEntry,
			PrevLogTerm:  appendFuture.request.PrevLogTerm,
			Entries: lo.Map(appendFuture.request.Entries, func(log *raft.Log, _ int) *pb.Log {
				return &pb.Log{
					Index:      log.Index,
					Term:       log.Term,
					Type:       pb.LogType(log.Type),
					Data:       log.Data,
					Extensions: log.Extensions,
					AppendedAt: timestamppb.New(log.AppendedAt),
				}
			}),
			LeaderCommitIndex: appendFuture.request.LeaderCommitIndex,
		},
	})
	if err != nil {
		return nil, err
	}

	t.inflight <- appendFuture

	return appendFuture, nil
}

func (t *AppendPipeline) Consumer() <-chan raft.AppendFuture {
	return t.reply
}

func (t *AppendPipeline) Close() error {
	t.once.Do(func() {
		close(t.reply)
		close(t.inflight)
	})
	return nil
}

var _ raft.Transport = &Transport{}

type Transport struct {
	peer             *PeerContainer
	peers            *Map[raft.ServerAddress, pb.TransportServiceClient]
	consumer         chan raft.RPC
	heartbeatHandler func(rpc raft.RPC)
	dialOptions      []grpc.DialOption
	callOptions      []grpc.CallOption
}

func NewTransport(peer *PeerContainer, dialOptions []grpc.DialOption, callOptions []grpc.CallOption) *Transport {
	return &Transport{
		peer:             peer,
		peers:            NewMap[raft.ServerAddress, pb.TransportServiceClient](),
		consumer:         make(chan raft.RPC, 1),
		heartbeatHandler: nil,
		dialOptions:      dialOptions,
		callOptions:      callOptions,
	}
}

func (t *Transport) getPeer(serverAddress raft.ServerAddress) (pb.TransportServiceClient, error) {
	return t.peers.GetOrNew(serverAddress, func() (pb.TransportServiceClient, error) {
		client, err := grpc.DialContext(context.TODO(), string(serverAddress), t.dialOptions...)
		if err != nil {
			return nil, err
		}

		return pb.NewTransportServiceClient(client), nil
	})
}

func (t *Transport) Consumer() <-chan raft.RPC {
	return t.consumer
}

func (t *Transport) LocalAddr() raft.ServerAddress {
	return t.peer.ServerAddress
}

func (t *Transport) AppendEntriesPipeline(serverID raft.ServerID, serverAddress raft.ServerAddress) (raft.AppendPipeline, error) {
	client, err := t.getPeer(serverAddress)
	if err != nil {
		return nil, err
	}

	stream, err := client.AppendEntriesPipeline(context.TODO(), t.callOptions...)
	if err != nil {
		return nil, err
	}

	appendPipeline := &AppendPipeline{
		once:     &sync.Once{},
		reply:    make(chan raft.AppendFuture, 1),
		inflight: make(chan *AppendFuture, 1),
		stream:   stream,
	}

	go appendPipeline.process()

	return appendPipeline, nil
}

func (t *Transport) AppendEntries(serverID raft.ServerID, serverAddress raft.ServerAddress, request *raft.AppendEntriesRequest, response *raft.AppendEntriesResponse) error {
	client, err := t.getPeer(serverAddress)
	if err != nil {
		return err
	}

	ctx := context.TODO()
	if isHeartbeat(request) {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Second*5)
		defer cancel()
	}

	result, err := client.AppendEntries(ctx, &pb.AppendEntriesRequest{
		RPCHeader: &pb.RPCHeader{
			ProtocolVersion: pb.ProtocolVersion(request.ProtocolVersion),
			ID:              request.ID,
			Addr:            request.Addr,
		},
		Term:         request.Term,
		Leader:       request.Leader,
		PrevLogEntry: request.PrevLogEntry,
		PrevLogTerm:  request.PrevLogTerm,
		Entries: lo.Map(request.Entries, func(log *raft.Log, _ int) *pb.Log {
			return &pb.Log{
				Index:      log.Index,
				Term:       log.Term,
				Type:       pb.LogType(log.Type),
				Data:       log.Data,
				Extensions: log.Extensions,
				AppendedAt: timestamppb.New(log.AppendedAt),
			}
		}),
		LeaderCommitIndex: request.LeaderCommitIndex,
	}, t.callOptions...)
	if err != nil {
		return err
	}

	response.RPCHeader.ProtocolVersion = raft.ProtocolVersion(result.RPCHeader.ProtocolVersion)
	response.RPCHeader.ID = result.RPCHeader.ID
	response.RPCHeader.Addr = result.RPCHeader.Addr
	response.Term = result.Term
	response.LastLog = result.LastLog
	response.Success = result.Success
	response.NoRetryBackoff = result.NoRetryBackoff

	return nil
}

func (t *Transport) RequestVote(serverID raft.ServerID, serverAddress raft.ServerAddress, request *raft.RequestVoteRequest, response *raft.RequestVoteResponse) error {
	client, err := t.getPeer(serverAddress)
	if err != nil {
		return err
	}

	result, err := client.RequestVote(context.TODO(), &pb.RequestVoteRequest{
		RPCHeader: &pb.RPCHeader{
			ProtocolVersion: pb.ProtocolVersion(request.ProtocolVersion),
			ID:              request.ID,
			Addr:            request.Addr,
		},
		Term:               request.Term,
		Candidate:          request.Candidate,
		LastLogIndex:       request.LastLogIndex,
		LastLogTerm:        request.LastLogTerm,
		LeadershipTransfer: request.LeadershipTransfer,
	}, t.callOptions...)
	if err != nil {
		return err
	}

	response.RPCHeader.ProtocolVersion = raft.ProtocolVersion(result.RPCHeader.ProtocolVersion)
	response.RPCHeader.ID = result.RPCHeader.ID
	response.RPCHeader.Addr = result.RPCHeader.Addr
	response.Term = result.Term
	response.Peers = result.Peers
	response.Granted = result.Granted

	return nil
}

func (t *Transport) InstallSnapshot(serverID raft.ServerID, serverAddress raft.ServerAddress, request *raft.InstallSnapshotRequest, response *raft.InstallSnapshotResponse, data io.Reader) error {
	client, err := t.getPeer(serverAddress)
	if err != nil {
		return err
	}

	stream, err := client.InstallSnapshot(context.TODO(), t.callOptions...)
	if err != nil {
		return err
	}

	err = stream.Send(&pb.InstallSnapshotStream{
		Payload: &pb.InstallSnapshotStream_Request{
			Request: &pb.InstallSnapshotStream_InstallSnapshotRequest{
				RPCHeader: &pb.RPCHeader{
					ProtocolVersion: pb.ProtocolVersion(request.ProtocolVersion),
					ID:              request.ID,
					Addr:            request.Addr,
				},
				SnapshotVersion:    pb.SnapshotVersion(request.SnapshotVersion),
				Term:               request.Term,
				Leader:             request.Leader,
				LastLogIndex:       request.LastLogIndex,
				LastLogTerm:        request.LastLogTerm,
				Peers:              request.Peers,
				Configuration:      request.Configuration,
				ConfigurationIndex: request.ConfigurationIndex,
				Size:               request.Size,
			},
		},
	})
	if err != nil {
		return err
	}

	for {
		var buffer [4096]byte
		n, err := data.Read(buffer[:])
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		err = stream.Send(&pb.InstallSnapshotStream{
			Payload: &pb.InstallSnapshotStream_Data{
				Data: buffer[:n],
			},
		})
		if err != nil {
			return err
		}
	}

	result, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}

	response.RPCHeader.ProtocolVersion = raft.ProtocolVersion(result.RPCHeader.ProtocolVersion)
	response.RPCHeader.ID = result.RPCHeader.ID
	response.RPCHeader.Addr = result.RPCHeader.Addr
	response.Term = result.Term
	response.Success = result.Success

	return nil
}

func (t *Transport) EncodePeer(serverID raft.ServerID, serverAddress raft.ServerAddress) []byte {
	data, _ := json.Marshal(&PeerContainer{
		ServerID:      serverID,
		ServerAddress: serverAddress,
	})
	return data
}

func (t *Transport) DecodePeer(data []byte) raft.ServerAddress {
	peer := PeerContainer{}
	_ = json.Unmarshal(data, &peer)
	return peer.ServerAddress
}

func (t *Transport) SetHeartbeatHandler(handler func(rpc raft.RPC)) {
	t.heartbeatHandler = handler
}

func (t *Transport) TimeoutNow(serverID raft.ServerID, serverAddress raft.ServerAddress, request *raft.TimeoutNowRequest, response *raft.TimeoutNowResponse) error {
	client, err := t.getPeer(serverAddress)
	if err != nil {
		return err
	}

	result, err := client.TimeoutNow(context.TODO(), &pb.TimeoutNowRequest{
		RPCHeader: &pb.RPCHeader{
			ProtocolVersion: pb.ProtocolVersion(request.ProtocolVersion),
			ID:              request.ID,
			Addr:            request.Addr,
		},
	}, t.callOptions...)
	if err != nil {
		return err
	}

	response.RPCHeader.ProtocolVersion = raft.ProtocolVersion(result.RPCHeader.ProtocolVersion)
	response.RPCHeader.ID = result.RPCHeader.ID
	response.RPCHeader.Addr = result.RPCHeader.Addr

	return nil
}

func (t *Transport) RegisterService(server *grpc.Server) {
	pb.RegisterTransportServiceServer(server, &Service{
		transport: t,
	})
}

var _ pb.TransportServiceServer = &Service{}

type Service struct {
	pb.UnimplementedTransportServiceServer
	transport *Transport
}

func (t *Service) process(ctx context.Context, command any, reader io.Reader) (any, error) {
	respChan := make(chan raft.RPCResponse, 1)

	rpc := raft.RPC{
		Command:  command,
		Reader:   reader,
		RespChan: respChan,
	}

	if isHeartbeat(command) && t.transport.heartbeatHandler != nil {
		t.transport.heartbeatHandler(rpc)
	} else {
		select {
		case <-ctx.Done():
			return nil, context.Canceled
		case t.transport.consumer <- rpc:
		}
	}

	select {
	case <-ctx.Done():
		return nil, context.Canceled
	case response := <-respChan:
		if response.Error != nil {
			return nil, response.Error
		}
		return response.Response, nil
	}
}

func (t *Service) AppendEntriesPipeline(server pb.TransportService_AppendEntriesPipelineServer) error {
	for {
		request, err := server.Recv()
		if err != nil {
			return err
		}

		result, err := t.process(server.Context(), &raft.AppendEntriesRequest{
			RPCHeader: raft.RPCHeader{
				ProtocolVersion: raft.ProtocolVersion(request.AppendEntriesRequest.RPCHeader.ProtocolVersion),
				ID:              request.AppendEntriesRequest.RPCHeader.ID,
				Addr:            request.AppendEntriesRequest.RPCHeader.Addr,
			},
			Term:         request.AppendEntriesRequest.Term,
			Leader:       request.AppendEntriesRequest.Leader,
			PrevLogEntry: request.AppendEntriesRequest.PrevLogEntry,
			PrevLogTerm:  request.AppendEntriesRequest.PrevLogTerm,
			Entries: lo.Map(request.AppendEntriesRequest.Entries, func(log *pb.Log, _ int) *raft.Log {
				return &raft.Log{
					Index:      log.Index,
					Term:       log.Term,
					Type:       raft.LogType(log.Type),
					Data:       log.Data,
					Extensions: log.Extensions,
					AppendedAt: log.AppendedAt.AsTime(),
				}
			}),
			LeaderCommitIndex: request.AppendEntriesRequest.LeaderCommitIndex,
		}, nil)

		var reply *pb.AppendEntriesPipelineResponse
		if err != nil {
			reply = &pb.AppendEntriesPipelineResponse{
				RequestID:             request.RequestID,
				Error:                 err.Error(),
				AppendEntriesResponse: nil,
			}
		} else {
			response, ok := result.(*raft.AppendEntriesResponse)
			if !ok {
				return fmt.Errorf("invalid response")
			}

			reply = &pb.AppendEntriesPipelineResponse{
				RequestID: request.RequestID,
				Error:     "",
				AppendEntriesResponse: &pb.AppendEntriesResponse{
					RPCHeader: &pb.RPCHeader{
						ProtocolVersion: pb.ProtocolVersion(response.RPCHeader.ProtocolVersion),
						ID:              response.RPCHeader.ID,
						Addr:            response.RPCHeader.Addr,
					},
					Term:           response.Term,
					LastLog:        response.LastLog,
					Success:        response.Success,
					NoRetryBackoff: response.NoRetryBackoff,
				},
			}
		}
		if err := server.Send(reply); err != nil {
			return err
		}
	}
}

func (t *Service) AppendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	result, err := t.process(ctx, &raft.AppendEntriesRequest{
		RPCHeader: raft.RPCHeader{
			ProtocolVersion: raft.ProtocolVersion(request.RPCHeader.ProtocolVersion),
			ID:              request.RPCHeader.ID,
			Addr:            request.RPCHeader.Addr,
		},
		Term:         request.Term,
		Leader:       request.Leader,
		PrevLogEntry: request.PrevLogEntry,
		PrevLogTerm:  request.PrevLogTerm,
		Entries: lo.Map(request.Entries, func(log *pb.Log, _ int) *raft.Log {
			return &raft.Log{
				Index:      log.Index,
				Term:       log.Term,
				Type:       raft.LogType(log.Type),
				Data:       log.Data,
				Extensions: log.Extensions,
				AppendedAt: log.AppendedAt.AsTime(),
			}
		}),
		LeaderCommitIndex: request.LeaderCommitIndex,
	}, nil)
	if err != nil {
		return nil, err
	}

	response, ok := result.(*raft.AppendEntriesResponse)
	if !ok {
		return nil, fmt.Errorf("invalid response")
	}

	return &pb.AppendEntriesResponse{
		RPCHeader: &pb.RPCHeader{
			ProtocolVersion: pb.ProtocolVersion(response.RPCHeader.ProtocolVersion),
			ID:              response.RPCHeader.ID,
			Addr:            response.RPCHeader.Addr,
		},
		Term:           response.Term,
		LastLog:        response.LastLog,
		Success:        response.Success,
		NoRetryBackoff: response.NoRetryBackoff,
	}, nil
}

func (t *Service) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	result, err := t.process(ctx, &raft.RequestVoteRequest{
		RPCHeader: raft.RPCHeader{
			ProtocolVersion: raft.ProtocolVersion(request.RPCHeader.ProtocolVersion),
			ID:              request.RPCHeader.ID,
			Addr:            request.RPCHeader.Addr,
		},
		Term:               request.Term,
		Candidate:          request.Candidate,
		LastLogIndex:       request.LastLogIndex,
		LastLogTerm:        request.LastLogTerm,
		LeadershipTransfer: request.LeadershipTransfer,
	}, nil)
	if err != nil {
		return nil, err
	}

	response, ok := result.(*raft.RequestVoteResponse)
	if !ok {
		return nil, fmt.Errorf("invalid response")
	}

	return &pb.RequestVoteResponse{
		RPCHeader: &pb.RPCHeader{
			ProtocolVersion: pb.ProtocolVersion(response.RPCHeader.ProtocolVersion),
			ID:              response.RPCHeader.ID,
			Addr:            response.RPCHeader.Addr,
		},
		Term:    response.Term,
		Peers:   response.Peers,
		Granted: response.Granted,
	}, nil
}

type snapshotReader struct {
	server pb.TransportService_InstallSnapshotServer
	buffer []byte
}

func (r *snapshotReader) Read(buffer []byte) (int, error) {
	if len(r.buffer) > 0 {
		n := copy(buffer, r.buffer)
		r.buffer = r.buffer[n:]
		return n, nil
	}

	reply, err := r.server.Recv()
	if err != nil {
		return 0, err
	}

	n := copy(buffer, reply.GetData())
	if n < len(reply.GetData()) {
		r.buffer = reply.GetData()[n:]
	}

	return n, nil
}

func (t *Service) InstallSnapshot(server pb.TransportService_InstallSnapshotServer) error {
	reply, err := server.Recv()
	if err != nil {
		return err
	}

	var (
		command any
		reader  io.Reader
	)

	if request := reply.GetRequest(); request != nil {
		command = &raft.InstallSnapshotRequest{
			RPCHeader: raft.RPCHeader{
				ProtocolVersion: raft.ProtocolVersion(request.RPCHeader.ProtocolVersion),
				ID:              request.RPCHeader.ID,
				Addr:            request.RPCHeader.Addr,
			},
			SnapshotVersion:    raft.SnapshotVersion(request.SnapshotVersion),
			Term:               request.Term,
			Leader:             request.Leader,
			LastLogIndex:       request.LastLogIndex,
			LastLogTerm:        request.LastLogTerm,
			Peers:              request.Peers,
			Configuration:      request.Configuration,
			ConfigurationIndex: request.ConfigurationIndex,
			Size:               request.Size,
		}
	}

	if data := reply.GetData(); len(data) > 0 {
		reader = &snapshotReader{
			server: server,
		}
	}

	result, err := t.process(server.Context(), command, reader)
	if err != nil {
		return err
	}

	response, ok := result.(*raft.InstallSnapshotResponse)
	if !ok {
		return fmt.Errorf("invalid response")
	}

	return server.SendAndClose(&pb.InstallSnapshotResponse{
		RPCHeader: &pb.RPCHeader{
			ProtocolVersion: pb.ProtocolVersion(response.RPCHeader.ProtocolVersion),
			ID:              response.RPCHeader.ID,
			Addr:            response.RPCHeader.Addr,
		},
		Term:    response.Term,
		Success: response.Success,
	})
}

func (t *Service) TimeoutNow(ctx context.Context, request *pb.TimeoutNowRequest) (*pb.TimeoutNowResponse, error) {
	result, err := t.process(ctx, &raft.TimeoutNowRequest{
		RPCHeader: raft.RPCHeader{
			ProtocolVersion: raft.ProtocolVersion(request.RPCHeader.ProtocolVersion),
			ID:              request.RPCHeader.ID,
			Addr:            request.RPCHeader.Addr,
		},
	}, nil)
	if err != nil {
		return nil, err
	}

	response, ok := result.(*raft.TimeoutNowResponse)
	if !ok {
		return nil, fmt.Errorf("invalid response")
	}

	return &pb.TimeoutNowResponse{
		RPCHeader: &pb.RPCHeader{
			ProtocolVersion: pb.ProtocolVersion(response.RPCHeader.ProtocolVersion),
			ID:              response.RPCHeader.ID,
			Addr:            response.RPCHeader.Addr,
		},
	}, nil
}

func isHeartbeat(command any) bool {
	request, ok := command.(*raft.AppendEntriesRequest)
	if !ok {
		return false
	}

	return request.Term != 0 && len(request.Leader) != 0 && request.PrevLogEntry == 0 && request.PrevLogTerm == 0 && len(request.Entries) == 0 && request.LeaderCommitIndex == 0
}
