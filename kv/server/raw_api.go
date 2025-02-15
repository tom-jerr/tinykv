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
	cf := req.GetCf()
	k := req.GetKey()
	reader, err := server.storage.Reader(nil)
	reply := &kvrpcpb.RawGetResponse{}
	if err != nil {
		reply.Error = err.Error()
		return reply, err
	}
	val, err := reader.GetCF(cf, k)
	if err != nil {
		reply.NotFound = true
		return reply, err
	}
	if val == nil {
		reply.NotFound = true
		return reply, nil
	}
	reply.Value = val
	reply.NotFound = false
	return reply, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}
	modify := storage.Modify{Data: put}

	err := server.storage.Write(nil, []storage.Modify{modify})
	if err != nil {
		return &kvrpcpb.RawPutResponse{Error: err.Error()}, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
	// return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	del := storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}
	modify := storage.Modify{Data: del}

	err := server.storage.Write(nil, []storage.Modify{modify})
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{Error: err.Error()}, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	startKey := req.GetStartKey()
	limit := req.GetLimit()
	cf := req.GetCf()
	var nums uint32 = 0
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return &kvrpcpb.RawScanResponse{Error: err.Error()}, err
	}
	iter := reader.IterCF(cf)

	resp := &kvrpcpb.RawScanResponse{}
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		if nums >= limit {
			break
		}
		item := iter.Item()
		val, err := item.Value()
		if err != nil {
			return &kvrpcpb.RawScanResponse{Error: err.Error()}, err
		}
		if val == nil {
			continue
		}
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{Key: item.Key(), Value: val})
		nums++
	}
	iter.Close()
	return resp, nil
}
