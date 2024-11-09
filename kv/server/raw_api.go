package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil || value == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}
	return &kvrpcpb.RawGetResponse{Value: value}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	m := storage.Put{Key: req.Key, Value: req.Value, Cf: req.Cf}
	ms := []storage.Modify{{Data: m}}
	ms = append(ms, storage.Modify{Data: m})
	err := server.storage.Write(nil, ms)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	ms := []storage.Modify{{Data: storage.Delete{Key: req.Key, Cf: req.Cf}}}
	err := server.storage.Write(nil, ms)
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{}, nil
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	var kvs []*kvrpcpb.KvPair
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		val, _ := item.Value()
		kvs = append(kvs, &kvrpcpb.KvPair{Key: item.Key(), Value: val})
		if uint32(len(kvs)) == req.Limit {
			break
		}
	}
	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}
