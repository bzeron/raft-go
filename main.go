package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"raft/transport"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/samber/lo"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	options = Options{
		NodeID:               uuid.New().String(),
		ServerAddress:        ":8080",
		RaftBindAddress:      ":8081",
		RaftAdvertiseAddress: "127.0.0.1:8081",
		RaftClusters:         nil,
		DataVolume:           "",
	}

	fsmStore = &FSMStore{
		data: transport.NewMap[string, string](),
	}
)

//go:generate protoc --proto_path=./transport/pb --go_out=paths=source_relative:./transport/pb --go-grpc_out=paths=source_relative:./transport/pb transport/pb/transport.proto
func main() {
	cmd := cobra.Command{
		Use: "RAFT",
		RunE: func(cmd *cobra.Command, args []string) error {
			err := os.MkdirAll(options.DataVolume, os.ModePerm)
			if err != nil {
				return err
			}

			raftConfig := raft.DefaultConfig()
			raftConfig.LocalID = raft.ServerID(options.NodeID)
			raftConfig.SnapshotInterval = 20 * time.Second
			raftConfig.SnapshotThreshold = 2

			logStore, err := raftboltdb.NewBoltStore(filepath.Join(options.DataVolume, "log.bolt"))
			if err != nil {
				return err
			}

			stableStore, err := raftboltdb.NewBoltStore(filepath.Join(options.DataVolume, "stable.bolt"))
			if err != nil {
				return err
			}

			snapshotStore, err := raft.NewFileSnapshotStore(options.DataVolume, 3, &LoggerImplWriter{slog.LevelDebug})
			if err != nil {
				return err
			}

			grpcTransport := transport.NewTransport(&transport.PeerContainer{
				ServerID:      raft.ServerID(options.NodeID),
				ServerAddress: raft.ServerAddress(options.RaftAdvertiseAddress),
			}, []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, []grpc.CallOption{})

			raftServer, err := raft.NewRaft(raftConfig, fsmStore, logStore, stableStore, snapshotStore, grpcTransport)
			if err != nil {
				return err
			}

			raftServer.BootstrapCluster(raft.Configuration{
				Servers: lo.Map(options.RaftClusters, func(address string, _ int) raft.Server {
					serverID, serverAddress, _ := strings.Cut(address, "=")
					return raft.Server{
						ID:      raft.ServerID(strings.TrimSpace(serverID)),
						Address: raft.ServerAddress(strings.TrimSpace(serverAddress)),
					}
				}),
			})

			r := gin.Default()
			r.POST("/node/join", IfError(NodeJoin(raftServer)))
			r.POST("/node/remove", IfError(NodeRemove(raftServer)))
			r.GET("/node/stats", IfError(NodeStats(raftServer)))
			r.GET("/store/:key", IfError(StoreGet(raftServer)))
			r.POST("/store", IfError(StoreSet(raftServer)))
			r.DELETE("/store/:key", IfError(StoreDel(raftServer)))

			g, ctx := errgroup.WithContext(cmd.Context())
			g.Go(func() error {
				server := grpc.NewServer()

				grpcTransport.RegisterService(server)

				listener, err := net.Listen("tcp", options.RaftBindAddress)
				if err != nil {
					return err
				}

				g.Go(func() error {
					select {
					case <-ctx.Done():
						server.Stop()
					}
					return nil
				})

				return server.Serve(listener)
			})
			g.Go(func() error {
				return r.Run(options.ServerAddress)
			})
			return g.Wait()
		},
	}
	cmd.Flags().StringVar(&options.NodeID, "node-id", "", "")
	cmd.Flags().StringVar(&options.ServerAddress, "server-address", "", "")
	cmd.Flags().StringVar(&options.RaftBindAddress, "raft-bind-address", "", "")
	cmd.Flags().StringVar(&options.RaftAdvertiseAddress, "raft-advertise-address", "", "")
	cmd.Flags().StringArrayVar(&options.RaftClusters, "raft-clusters", nil, "")
	cmd.Flags().StringVar(&options.DataVolume, "data-volume", "", "")
	cobra.CheckErr(cmd.Execute())
}

type Options struct {
	NodeID               string
	RaftBindAddress      string
	RaftAdvertiseAddress string
	RaftClusters         []string
	ServerAddress        string
	DataVolume           string
}

type LoggerImplWriter struct {
	level slog.Level
}

func (l LoggerImplWriter) Write(msg []byte) (int, error) {
	slog.Log(context.Background(), l.level, string(msg))
	return len(msg), nil
}

const (
	GET Operation = "GET"
	SET Operation = "SET"
	DEL Operation = "DEL"
)

type Operation string

type CommandPayload struct {
	Operation Operation
	Key       string
	Value     string
}

type CommandStats struct {
	Error     error
	Operation Operation
	Key       string
	Value     string
}

type WarpHandlerFunc func(c *gin.Context) (int, any, error)

func IfError(handler WarpHandlerFunc) gin.HandlerFunc {
	type Response struct {
		Err  string
		Msg  string
		Data any
	}
	return func(c *gin.Context) {
		response := &Response{}
		status, data, err := handler(c)
		if err != nil {
			response.Err = err.Error()
			response.Msg = "Error"
		} else {
			response.Msg = "OK"
			response.Data = data
		}
		c.JSON(status, response)
	}
}

func NodeJoin(srv *raft.Raft) WarpHandlerFunc {
	return func(c *gin.Context) (int, any, error) {
		var form struct {
			NodeID      string `json:"nodeID"`
			RaftAddress string `json:"raftAddress"`
		}

		err := c.ShouldBind(&form)
		if err != nil {
			return http.StatusUnprocessableEntity, nil, err
		}

		if srv.State() != raft.Leader {
			return http.StatusUnprocessableEntity, nil, fmt.Errorf("must the leader")
		}

		err = srv.GetConfiguration().Error()
		if err != nil {
			return http.StatusUnprocessableEntity, nil, err
		}

		err = srv.AddVoter(raft.ServerID(form.NodeID), raft.ServerAddress(form.RaftAddress), 0, 0).Error()
		if err != nil {
			return http.StatusUnprocessableEntity, nil, err
		}

		return http.StatusOK, srv.Stats(), nil
	}
}

func NodeRemove(srv *raft.Raft) WarpHandlerFunc {
	return func(c *gin.Context) (int, any, error) {
		var form struct {
			NodeID string `json:"nodeID"`
		}

		err := c.ShouldBind(&form)
		if err != nil {
			return http.StatusUnprocessableEntity, nil, err
		}

		if srv.State() != raft.Leader {
			return http.StatusUnprocessableEntity, nil, fmt.Errorf("must the leader")
		}

		err = srv.GetConfiguration().Error()
		if err != nil {
			return http.StatusUnprocessableEntity, nil, err
		}

		err = srv.RemoveServer(raft.ServerID(form.NodeID), 0, 0).Error()
		if err != nil {
			return http.StatusUnprocessableEntity, nil, err
		}

		return http.StatusOK, srv.Stats(), nil
	}
}

func NodeStats(srv *raft.Raft) WarpHandlerFunc {
	return func(c *gin.Context) (int, any, error) {
		serverAddress, serverID := srv.LeaderWithID()
		fmt.Println(serverAddress, serverID)

		return http.StatusOK, srv.Stats(), nil
	}
}

func Payload(srv *raft.Raft, payload *CommandPayload) WarpHandlerFunc {
	return func(c *gin.Context) (int, any, error) {
		if payload.Operation == GET {
			value, err := fsmStore.data.Get(payload.Key)
			if err != nil {
				return http.StatusUnprocessableEntity, nil, err
			}

			stats := &CommandStats{
				Operation: GET,
				Key:       payload.Key,
				Value:     value,
			}

			return http.StatusOK, stats, nil
		}

		if srv.State() != raft.Leader {
			return http.StatusUnprocessableEntity, nil, fmt.Errorf("must the leader")
		}

		data, err := json.Marshal(payload)
		if err != nil {
			return http.StatusUnprocessableEntity, nil, err
		}

		applyFuture := srv.Apply(data, 500*time.Millisecond)

		err = applyFuture.Error()
		if err != nil {
			return http.StatusUnprocessableEntity, nil, err
		}

		stats, ok := applyFuture.Response().(*CommandStats)

		if !ok {
			return http.StatusUnprocessableEntity, nil, fmt.Errorf("response is not match apply response")
		}

		if stats.Error != nil {
			return http.StatusUnprocessableEntity, nil, stats.Error
		}

		return http.StatusOK, stats, nil
	}
}

func StoreGet(srv *raft.Raft) WarpHandlerFunc {
	return func(c *gin.Context) (int, any, error) {
		return Payload(srv, &CommandPayload{
			Operation: GET,
			Key:       c.Param("key"),
			Value:     "",
		})(c)
	}
}

func StoreSet(srv *raft.Raft) WarpHandlerFunc {
	return func(c *gin.Context) (int, any, error) {
		var form struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}

		err := c.ShouldBind(&form)
		if err != nil {
			return http.StatusUnprocessableEntity, nil, err
		}

		return Payload(srv, &CommandPayload{
			Operation: SET,
			Key:       form.Key,
			Value:     form.Value,
		})(c)
	}
}

func StoreDel(srv *raft.Raft) WarpHandlerFunc {
	return func(c *gin.Context) (int, any, error) {
		return Payload(srv, &CommandPayload{
			Operation: DEL,
			Key:       c.Param("key"),
			Value:     "",
		})(c)
	}
}

var _ raft.FSMSnapshot = &FSMSnapshot{}

type FSMSnapshot struct {
	store *FSMStore
}

func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	defer func() {
		err := sink.Close()
		if err != nil {
			slog.Error("[FINALLY SNAPSHOT] close error", err)
		}
	}()

	snapshot, err := s.store.data.Marshal()
	if err != nil {
		slog.Error("[END SNAPSHOT] marshal error", err)
		_ = sink.Cancel()
		return err
	}

	_, err = sink.Write(snapshot)
	if err != nil {
		slog.Error("[END SNAPSHOT] write error", err)
		_ = sink.Cancel()
		return err
	}

	return nil
}

func (s *FSMSnapshot) Release() {

}

var _ raft.FSM = &FSMStore{}

type FSMStore struct {
	data *transport.Map[string, string]
}

func (s *FSMStore) Apply(log *raft.Log) any {
	switch log.Type {
	case raft.LogCommand:
		payload := CommandPayload{}

		err := json.Unmarshal(log.Data, &payload)
		if err != nil {
			return CommandStats{
				Error: err,
			}
		}

		slog.Info("[START APPLY] from snapshot", slog.AnyValue(payload))

		switch payload.Operation {
		case GET:
			value, err := s.data.Get(payload.Key)
			return &CommandStats{
				Error:     err,
				Operation: GET,
				Key:       payload.Key,
				Value:     value,
			}

		case SET:
			return &CommandStats{
				Error:     s.data.Set(payload.Key, payload.Value),
				Operation: SET,
				Key:       payload.Key,
				Value:     payload.Value,
			}

		case DEL:
			value, err := s.data.Del(payload.Key)
			return &CommandStats{
				Error:     err,
				Operation: DEL,
				Key:       payload.Key,
				Value:     value,
			}
		}
	}

	return nil
}

func (s *FSMStore) Snapshot() (raft.FSMSnapshot, error) {
	return &FSMSnapshot{s}, nil
}

func (s *FSMStore) Restore(snapshot io.ReadCloser) error {
	defer func() {
		err := snapshot.Close()
		if err != nil {
			slog.Error("[FINALLY RESTORE] close error", err)
		}
	}()

	decoder := json.NewDecoder(snapshot)
	for decoder.More() {
		payload := CommandPayload{}

		err := decoder.Decode(&payload)
		if err != nil {
			slog.Error("[END RESTORE] unmarshal error", err)
			return err
		}

		err = s.data.Set(payload.Key, payload.Value)
		if err != nil {
			slog.Error("[END RESTORE] set error", err)
			return err
		}
	}

	_, err := decoder.Token()
	if err != nil && err != io.EOF {
		slog.Error("[END RESTORE] persist error", err)
		return err
	}

	return nil
}
