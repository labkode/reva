package storageprovidergwsvc

import (
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/cernbox/reva/pkg/err"
	"github.com/cernbox/reva/pkg/log"
	"github.com/cernbox/reva/pkg/storage"
	"github.com/cernbox/reva/pkg/storage/local"
	"google.golang.org/grpc"

	rpcpb "github.com/cernbox/go-cs3apis/cs3/rpc"
	storagebrokerv0alphapb "github.com/cernbox/go-cs3apis/cs3/storagebroker/v0alpha"
	storageproviderv0alphapb "github.com/cernbox/go-cs3apis/cs3/storageprovider/v0alpha"

	"github.com/mitchellh/mapstructure"
	"golang.org/x/net/context"
)

var logger = log.New("storageprovidergwsvc")
var errors = err.New("storageprovidergwsvc")

type config struct {
	MountPath string `mapstructure:"mount_path"`
	MountID   string `mapstructure:"mount_id"`
	Broker    string `mapstructure:"broker"`
}

type service struct {
	c *config
}

func (s *service) getConn(uri string) (*grpc.ClientConn, error) {
	conn, err := grpc.Dial(s.c.Broker, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (s *service) getBrokerClient() (storagebrokerv0alphapb.StorageBrokerServiceClient, error) {
	conn, err := s.getConn(s.c.Broker)
	if err != nil {
		return nil, err
	}

	return storagebrokerv0alphapb.NewStorageBrokerServiceClient(conn), nil
}

func (s *service) getStorageClient(ctx context.Context, fn string) (storageproviderv0alphapb.StorageProviderServiceClient, error) {
	brokerClient, err := s.getBrokerClient()
	if err != nil {
		return nil, err
	}

	req := &storagebrokerv0alphapb.FindRequest{Filename: fn}
	res, err := brokerClient.Find(ctx, req)
	if err != nil {
		return nil, err
	}

	if res.Status.Code != rpcpb.Code_CODE_OK {
		return nil, errors.New("error obtaining storage provider from broker")
	}

	conn, err := s.getConn(res.StorageProvider.Endpoint)
	if err != nil {
		return nil, err
	}

	return storageproviderv0alphapb.NewStorageProviderServiceClient(conn), nil
}

// New creates a new storage provider svc
func New(m map[string]interface{}) (storageproviderv0alphapb.StorageProviderServiceServer, error) {

	c, err := parseConfig(m)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse config")
	}

	service := &service{
		c: c,
	}

	return service, nil
}

func (s *service) Deref(ctx context.Context, req *storageproviderv0alphapb.DerefRequest) (*storageproviderv0alphapb.DerefResponse, error) {
	return nil, nil
}

func (s *service) CreateDirectory(ctx context.Context, req *storageproviderv0alphapb.CreateDirectoryRequest) (*storageproviderv0alphapb.CreateDirectoryResponse, error) {
	client, err := s.getStorageClient(ctx, req.Filename)
	if err != nil {
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.CreateDirectoryResponse{Status: status}
		return res, nil
	}

	res, err := client.CreateDirectory(ctx, req)
	if err != nil {
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.CreateDirectoryResponse{Status: status}
		return res, nil
	}

	return res, nil
}

func (s *service) Delete(ctx context.Context, req *storageproviderv0alphapb.DeleteRequest) (*storageproviderv0alphapb.DeleteResponse, error) {
	client, err := s.getStorageClient(ctx, req.Filename)
	if err != nil {
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.DeleteResponse{Status: status}
		return res, nil
	}

	res, err := client.Delete(ctx, req)
	if err != nil {
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.DeleteResponse{Status: status}
		return res, nil
	}
	return res, nil
}

// TODO(labkode): maybe plug here the cross storage copy
func (s *service) Move(ctx context.Context, req *storageproviderv0alphapb.MoveRequest) (*storageproviderv0alphapb.MoveResponse, error) {
	sourceClient, err := s.getStorageClient(ctx, req.SourceFilename)
	if err != nil {
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.MoveResponse{Status: status}
		return res, nil
	}

	targetClient, err := s.getStorageClient(ctx, req.TargetFilename)
	if err != nil {
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.MoveResponse{Status: status}
		return res, nil
	}

	if sourceClient != targetClient {
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_UNIMPLEMENTED}
		res := &storageproviderv0alphapb.MoveResponse{Status: status}
		return res, nil
	}

	res, err := sourceClient.Move(ctx, req)
	if err != nil {
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.MoveResponse{Status: status}
		return res, nil
	}

	return res, nil
}

func (s *service) Stat(ctx context.Context, req *storageproviderv0alphapb.StatRequest) (*storageproviderv0alphapb.StatResponse, error) {
	client, err := s.getStorageClient(ctx, req.Filename)
	if err != nil {
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.StatResponse{Status: status}
		return res, nil
	}

	res, err := client.Stat(ctx, req)
	if err != nil {
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.StatResponse{Status: status}
		return res, nil
	}

	return res, nil
}

func (s *service) List(req *storageproviderv0alphapb.ListRequest, stream storageproviderv0alphapb.StorageProviderService_ListServer) error {
	ctx := stream.Context()

	client, err := s.getStorageClient(ctx, req.Filename)
	if err != nil {
		logger.Println(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.ListResponse{Status: status}
		if err = stream.Send(res); err != nil {
			return errors.Wrap(err, "error streaming response")
		}
		return nil
	}

	ss, err := client.List(ctx, req)
	if err != nil {
		logger.Println(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.ListResponse{Status: status}
		if err = stream.Send(res); err != nil {
			return errors.Wrap(err, "error streaming response")
		}
		return nil
	}

	for {
		res, err := ss.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			logger.Println(ctx, err)
			status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
			res := &storageproviderv0alphapb.ListResponse{Status: status}
			if err = stream.Send(res); err != nil {
				return errors.Wrap(err, "error streaming response")
			}
			return nil
		}

		if err := stream.Send(res); err != nil {
			return errors.Wrap(err, "error streaming response")
		}
	}

	return nil
}

func (s *service) StartWriteSession(ctx context.Context, req *storageproviderv0alphapb.StartWriteSessionRequest) (*storageproviderv0alphapb.StartWriteSessionResponse, error) {
	client, err := s.getStorageClient(ctx, req.Filename)
	if err != nil {
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.StartWriteSessionResponse{Status: status}
		return res, nil
	}

	res, err := client.StartWriteSession(ctx, req)
	if err != nil {
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.StartWriteSessionResponse{Status: status}
		return res, nil
	}

	return res, nil
}

func (s *service) Write(stream storageproviderv0alphapb.StorageProviderService_WriteServer) error {
	ctx := stream.Context()
	var numChunks int
	var writtenBytes int64

	var writeClient storageproviderv0alphapb.StorageProviderService_WriteClient
	for {
		req, err := stream.Recv()

		if err == io.EOF {
			logger.Println(ctx, "no more chunks to receive")
			break
		}

		if err != nil {
			err = errors.Wrap(err, "error receiving write request")
			logger.Error(ctx, err)

			status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
			res := &storageproviderv0alphapb.WriteResponse{Status: status}
			if err = stream.SendAndClose(res); err != nil {
				err = errors.Wrap(err, "error closing stream for write")
				return err
			}
			return nil
		}

		if writeClient == nil {
			client, err := s.getStorageClient(ctx, req.SessionId)
			if err != nil {
				status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
				res := &storageproviderv0alphapb.WriteResponse{Status: status}
				if err = stream.SendAndClose(res); err != nil {
					err = errors.Wrap(err, "error closing stream for write")
					return err
				}
				return nil
			}
			wc, err := client.Write(ctx)
			if err != nil {
				status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
				res := &storageproviderv0alphapb.WriteResponse{Status: status}
				if err = stream.SendAndClose(res); err != nil {
					err = errors.Wrap(err, "error closing stream for write")
					return err
				}
				return nil
			}
			writeClient = wc
		}

		if err := writeClient.Send(req); err != nil {
			status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
			res := &storageproviderv0alphapb.WriteResponse{Status: status}
			if err = stream.SendAndClose(res); err != nil {
				err = errors.Wrap(err, "error streaming")
				return err
			}
			return nil
		}
	}

	res, err := writeClient.CloseAndRecv()
	if err != nil {
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.WriteResponse{Status: status}
		if err = stream.SendAndClose(res); err != nil {
			err = errors.Wrap(err, "error streaming")
			return err
		}
		return nil
	}

	if res.Status.Code != rpcpb.Code_CODE_OK {
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.WriteResponse{Status: status}
		if err = stream.SendAndClose(res); err != nil {
			err = errors.Wrap(err, "error streaming")
			return err
		}
		return nil
	}
	return nil
}

func (s *service) FinishWriteSession(ctx context.Context, req *storageproviderv0alphapb.FinishWriteSessionRequest) (*storageproviderv0alphapb.FinishWriteSessionResponse, error) {
	client, err := s.getStorageClient(ctx, req.SessionId)
	if err != nil {
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.FinishWriteSessionResponse{Status: status}
		return res, nil
	}

	res, err := client.FinishWriteSession(ctx, req)
	if err != nil {
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.FinishWriteSessionResponse{Status: status}
		return res, nil
	}

	return res, nil
}

func (s *service) Read(req *storageproviderv0alphapb.ReadRequest, stream storageproviderv0alphapb.StorageProviderService_ReadServer) error {
	ctx := stream.Context()

	client, err := s.getStorageClient(ctx, req.Filename)
	if err != nil {
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.ReadResponse{Status: status}
		if err = stream.Send(res); err != nil {
			return errors.Wrap(err, "error streaming")
		}
		return nil
	}

	readStream, err := client.Read(ctx, req)
	if err != nil {
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.ReadResponse{Status: status}
		if err = stream.Send(res); err != nil {
			return errors.Wrap(err, "error streaming")
		}
		return nil
	}

	for {
		res, err := readStream.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			err = errors.Wrap(err, "error receiving from upstream")
			logger.Error(ctx, err)
			status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
			res := &storageproviderv0alphapb.ReadResponse{Status: status}
			if err = stream.Send(res); err != nil {
				return errors.Wrap(err, "error streaming")
			}
			return nil
		}

		if res.Status.Code != rpcpb.Code_CODE_OK {
			status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
			res := &storageproviderv0alphapb.ReadResponse{Status: status}
			if err = stream.Send(res); err != nil {
				return errors.Wrap(err, "error streaming")
			}
			return nil
		}

		if err := stream.Send(res); err != nil {
			logger.Error(ctx, err)
			status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
			res := &storageproviderv0alphapb.ReadResponse{Status: status}
			if err = stream.Send(res); err != nil {
				return errors.Wrap(err, "error streaming")
			}
			return nil
		}
	}

	return nil
}

func (s *service) ListVersions(req *storageproviderv0alphapb.ListVersionsRequest, stream storageproviderv0alphapb.StorageProviderService_ListVersionsServer) error {
	ctx := stream.Context()
	revs, err := s.storage.ListRevisions(ctx, req.Filename)
	if err != nil {
		err = errors.Wrap(err, "storageprovidersvc: error listing revisions")
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.ListVersionsResponse{Status: status}
		if err = stream.Send(res); err != nil {
			return errors.Wrap(err, "storageprovidersvc: error streaming list versions response")
		}
		return nil
	}

	for _, rev := range revs {
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
		version := &storageproviderv0alphapb.Version{
			Key:   rev.RevKey,
			IsDir: rev.IsDir,
			Mtime: rev.Mtime,
			Size:  rev.Size,
		}
		res := &storageproviderv0alphapb.ListVersionsResponse{Status: status, Version: version}
		if err := stream.Send(res); err != nil {
			return errors.Wrap(err, "storageprovidersvc: error streaming list versions response")
		}
	}
	return nil
}

func (s *service) ReadVersion(req *storageproviderv0alphapb.ReadVersionRequest, stream storageproviderv0alphapb.StorageProviderService_ReadVersionServer) error {
	ctx := stream.Context()
	fd, err := s.storage.DownloadRevision(ctx, req.Filename, req.VersionKey)
	defer func() {
		if err := fd.Close(); err != nil {
			err = errors.Wrap(err, "storageprovidersvc: error closing fd for version file - leak")
			logger.Error(ctx, err)
			// continue
		}
	}()

	if err != nil {
		err = errors.Wrap(err, "storageprovidersvc: error downloading revision")
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.ReadVersionResponse{Status: status}
		if err = stream.Send(res); err != nil {
			return errors.Wrap(err, "storageprovidersvc: error streaming read version response")
		}
		return nil
	}

	// send data chunks of maximum 1 MiB
	buffer := make([]byte, 1024*1024*3)
	for {
		n, err := fd.Read(buffer)
		if n > 0 {
			dc := &storageproviderv0alphapb.DataChunk{Data: buffer[:n], Length: uint64(n)}
			status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
			res := &storageproviderv0alphapb.ReadVersionResponse{Status: status, DataChunk: dc}
			if err = stream.Send(res); err != nil {
				return errors.Wrap(err, "storageprovidersvc: error streaming read version response")
			}
		}

		// nothing more to send
		if err == io.EOF {
			break
		}

		if err != nil {
			err = errors.Wrap(err, "storageprovidersvc: error reading from fd")
			logger.Error(ctx, err)
			status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
			res := &storageproviderv0alphapb.ReadVersionResponse{Status: status}
			if err = stream.Send(res); err != nil {
				return errors.Wrap(err, "storageprovidersvc: error streaming read response")
			}
			return nil
		}
	}

	return nil

}

func (s *service) RestoreVersion(ctx context.Context, req *storageproviderv0alphapb.RestoreVersionRequest) (*storageproviderv0alphapb.RestoreVersionResponse, error) {
	client, err := s.getStorageClient(ctx, req.Filename)
	if err != nil {
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.RestoreVersionResponse{Status: status}
		return res, nil
	}

	res, err := client.RestoreVersion(ctx, req)
	if err != nil {
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.RestoreVersionResponse{Status: status}
		return res, nil
	}

	return res, nil
}

func (s *service) ListRecycle(req *storageproviderv0alphapb.ListRecycleRequest, stream storageproviderv0alphapb.StorageProviderService_ListRecycleServer) error {
	ctx := stream.Context()
	fn := req.GetFilename()

	items, err := s.storage.ListRecycle(ctx, fn)
	if err != nil {
		err := errors.Wrap(err, "storageprovidersvc: error listing recycle")
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.ListRecycleResponse{Status: status}
		if err = stream.Send(res); err != nil {
			return errors.Wrap(err, "storageprovidersvc: error streaming list recycle response")
		}
	}

	for _, item := range items {
		recycleItem := &storageproviderv0alphapb.RecycleItem{
			Filename: item.RestorePath,
			Key:      item.RestoreKey,
			Size:     item.Size,
			Deltime:  item.DelMtime,
			IsDir:    item.IsDir,
		}
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
		res := &storageproviderv0alphapb.ListRecycleResponse{
			Status:      status,
			RecycleItem: recycleItem,
		}

		if err := stream.Send(res); err != nil {
			return errors.Wrap(err, "storageprovidersvc: error streaming list recycle response")
		}
	}

	return nil
}

func (s *service) RestoreRecycleItem(ctx context.Context, req *storageproviderv0alphapb.RestoreRecycleItemRequest) (*storageproviderv0alphapb.RestoreRecycleItemResponse, error) {
	client, err := s.getStorageClient(ctx, req.Filename)
	if err != nil {
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.RestoreRecycleItemResponse{Status: status}
		return res, nil
	}

	res, err := client.RestoreRecycleItem(ctx, req)
	if err != nil {
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.RestoreRecycleItemResponse{Status: status}
		return res, nil
	}

	return res, nil
}

func (s *service) PurgeRecycle(ctx context.Context, req *storageproviderv0alphapb.PurgeRecycleRequest) (*storageproviderv0alphapb.PurgeRecycleResponse, error) {
	client, err := s.getStorageClient(ctx, req.Filename)
	if err != nil {
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.PurgeRecycleResponse{Status: status}
		return res, nil
	}

	res, err := client.
}

func (s *service) SetACL(ctx context.Context, req *storageproviderv0alphapb.SetACLRequest) (*storageproviderv0alphapb.SetACLResponse, error) {
	fn := req.Filename
	aclTarget := req.Acl.Target
	aclMode := s.getPermissions(req.Acl.Mode)
	aclType := s.getTargetType(req.Acl.Type)

	// check mode is valid
	if aclMode == storage.ACLModeInvalid {
		logger.Println(ctx, "acl mode is invalid")
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INVALID_ARGUMENT, Message: "acl mode is invalid"}
		res := &storageproviderv0alphapb.SetACLResponse{Status: status}
		return res, nil
	}

	// check targetType is valid
	if aclType == storage.ACLTypeInvalid {
		logger.Println(ctx, "acl  type is invalid")
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INVALID_ARGUMENT, Message: "acl type is invalid"}
		res := &storageproviderv0alphapb.SetACLResponse{Status: status}
		return res, nil
	}

	acl := &storage.ACL{
		Target: aclTarget,
		Mode:   aclMode,
		Type:   aclType,
	}

	err := s.storage.SetACL(ctx, fn, acl)
	if err != nil {
		err = errors.Wrap(err, "storageprovidersvc: error setting acl")
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.SetACLResponse{Status: status}
		return res, nil
	}

	status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
	res := &storageproviderv0alphapb.SetACLResponse{Status: status}
	return res, nil
}

func (s *service) getTargetType(t storageproviderv0alphapb.ACLType) storage.ACLType {
	switch t {
	case storageproviderv0alphapb.ACLType_ACL_TYPE_USER:
		return storage.ACLTypeUser
	case storageproviderv0alphapb.ACLType_ACL_TYPE_GROUP:
		return storage.ACLTypeGroup
	default:
		return storage.ACLTypeInvalid
	}
}

func (s *service) getPermissions(mode storageproviderv0alphapb.ACLMode) storage.ACLMode {
	switch mode {
	case storageproviderv0alphapb.ACLMode_ACL_MODE_READONLY:
		return storage.ACLModeReadOnly
	case storageproviderv0alphapb.ACLMode_ACL_MODE_READWRITE:
		return storage.ACLModeReadWrite
	default:
		return storage.ACLModeInvalid
	}
}

func (s *service) UpdateACL(ctx context.Context, req *storageproviderv0alphapb.UpdateACLRequest) (*storageproviderv0alphapb.UpdateACLResponse, error) {
	fn := req.Filename
	target := req.Acl.Target
	mode := s.getPermissions(req.Acl.Mode)
	targetType := s.getTargetType(req.Acl.Type)

	// check mode is valid
	if mode == storage.ACLModeInvalid {
		logger.Println(ctx, "acl mode is invalid")
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INVALID_ARGUMENT, Message: "acl mode is invalid"}
		res := &storageproviderv0alphapb.UpdateACLResponse{Status: status}
		return res, nil
	}

	// check targetType is valid
	if targetType == storage.ACLTypeInvalid {
		logger.Println(ctx, "acl  type is invalid")
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INVALID_ARGUMENT, Message: "acl type is invalid"}
		res := &storageproviderv0alphapb.UpdateACLResponse{Status: status}
		return res, nil
	}

	acl := &storage.ACL{
		Target: target,
		Mode:   mode,
		Type:   targetType,
	}

	if err := s.storage.UpdateACL(ctx, fn, acl); err != nil {
		err = errors.Wrap(err, "storageprovidersvc: error updating acl")
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.UpdateACLResponse{Status: status}
		return res, nil
	}
	status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
	res := &storageproviderv0alphapb.UpdateACLResponse{Status: status}
	return res, nil
}

func (s *service) UnsetACL(ctx context.Context, req *storageproviderv0alphapb.UnsetACLRequest) (*storageproviderv0alphapb.UnsetACLResponse, error) {
	fn := req.Filename
	aclTarget := req.Acl.Target
	aclType := s.getTargetType(req.Acl.Type)

	// check targetType is valid
	if aclType == storage.ACLTypeInvalid {
		logger.Println(ctx, "acl  type is invalid")
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INVALID_ARGUMENT, Message: "acl type is invalid"}
		res := &storageproviderv0alphapb.UnsetACLResponse{Status: status}
		return res, nil
	}

	acl := &storage.ACL{
		Target: aclTarget,
		Type:   aclType,
	}

	if err := s.storage.UnsetACL(ctx, fn, acl); err != nil {
		err = errors.Wrap(err, "storageprovidersvc: error unsetting acl")
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.UnsetACLResponse{Status: status}
		return res, nil
	}

	status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
	res := &storageproviderv0alphapb.UnsetACLResponse{Status: status}
	return res, nil
}

func (s *service) GetQuota(ctx context.Context, req *storageproviderv0alphapb.GetQuotaRequest) (*storageproviderv0alphapb.GetQuotaResponse, error) {
	total, used, err := s.storage.GetQuota(ctx, req.Filename)
	if err != nil {
		err = errors.Wrap(err, "storageprovidersvc: error getting quota")
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.GetQuotaResponse{Status: status}
		return res, nil
	}
	status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
	res := &storageproviderv0alphapb.GetQuotaResponse{Status: status, TotalBytes: uint64(total), UsedBytes: uint64(used)}
	return res, nil
}

func (s *service) splitFn(fsfn string) (string, string, error) {
	tokens := strings.Split(fsfn, "/")
	l := len(tokens)
	if l == 0 {
		return "", "", errors.New("fsfn is not id-based")
	}

	fid := tokens[0]
	if l > 1 {
		return fid, path.Join(tokens[1:]...), nil
	}
	return fid, "", nil
}

type fnCtx struct {
	mountPrefix string
	*derefCtx
}

type derefCtx struct {
	derefPath string
	fid       string
	rootFidFn string
}

func (s *service) deref(ctx context.Context, fsfn string) (*derefCtx, error) {
	if strings.HasPrefix(fsfn, "/") {
		return &derefCtx{derefPath: fsfn}, nil
	}

	fid, right, err := s.splitFn(fsfn)
	if err != nil {
		return nil, err
	}
	// resolve fid to path in the fs
	fnPointByID, err := s.storage.GetPathByID(ctx, fid)
	if err != nil {
		return nil, err
	}

	derefPath := path.Join(fnPointByID, right)
	return &derefCtx{derefPath: derefPath, fid: fid, rootFidFn: fnPointByID}, nil
}

func (s *service) unwrap(ctx context.Context, fn string) (string, *fnCtx, error) {
	mp, fsfn, err := s.trimMounPrefix(fn)
	if err != nil {
		return "", nil, err
	}

	derefCtx, err := s.deref(ctx, fsfn)
	if err != nil {
		return "", nil, err
	}

	fctx := &fnCtx{
		derefCtx:    derefCtx,
		mountPrefix: mp,
	}
	return fsfn, fctx, nil
}

func (s *service) wrap(ctx context.Context, fsfn string, fctx *fnCtx) string {
	if !strings.HasPrefix(fsfn, "/") {
		fsfn = strings.TrimPrefix(fsfn, fctx.rootFidFn)
		fsfn = path.Join(fctx.fid, fsfn)
		fsfn = fctx.mountPrefix + ":" + fsfn
	} else {
		fsfn = path.Join(fctx.mountPrefix, fsfn)
	}

	return fsfn
}

func (s *service) trimMounPrefix(fn string) (string, string, error) {
	mountID := s.mountID + ":"
	if strings.HasPrefix(fn, s.mountPath) {
		return s.mountPath, path.Join("/", strings.TrimPrefix(fn, s.mountPath)), nil
	}
	if strings.HasPrefix(fn, mountID) {
		return mountID, strings.TrimPrefix(fn, mountID), nil
	}
	return "", "", errors.New("fn does not belong to this storage provider: " + fn)
}

func parseConfig(m map[string]interface{}) (*config, error) {
	c := &config{}
	if err := mapstructure.Decode(m, c); err != nil {
		return nil, err
	}
	return c, nil
}

func getFS(c *config) (storage.FS, error) {
	switch c.Driver {
	case "local":
		return local.New(c.Local)
	case "":
		return nil, fmt.Errorf("driver is empty")
	default:
		return nil, fmt.Errorf("driver not found: %s", c.Driver)
	}
}

type notFoundError interface {
	IsNotFound()
}

func toPerm(p *storage.Permissions) *storageproviderv0alphapb.Permissions {
	return &storageproviderv0alphapb.Permissions{
		Read:  p.Read,
		Write: p.Write,
		Share: p.Share,
	}
}

func (s *service) toMeta(md *storage.MD) *storageproviderv0alphapb.Metadata {
	perm := toPerm(md.Permissions)
	meta := &storageproviderv0alphapb.Metadata{
		Filename:    md.Path,
		Checksum:    md.Checksum,
		Etag:        md.Etag,
		Id:          s.mountID + ":" + md.ID,
		IsDir:       md.IsDir,
		Mime:        md.Mime,
		Mtime:       md.Mtime,
		Size:        md.Size,
		Permissions: perm,
	}

	return meta
}
