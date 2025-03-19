package handler

import (
	"context"
	"github.com/linkypi/hiraeth.registry/common"
	"github.com/linkypi/hiraeth.registry/server/slot"
	"google.golang.org/protobuf/proto"
)

type FetchMetadataHandler struct {
	Handler
}

func (r *FetchMetadataHandler) GetRouteKey(_ proto.Message, ctx context.Context) string {
	return ""
}

func (r *FetchMetadataHandler) Handle(_ any, _ *slot.Bucket, ctx context.Context) (proto.Message, error) {

	res, err := r.serviceImpl.FetchMetadata()
	if err != nil {
		common.Errorf("failed to fetch meta data: %v", err)
		return nil, err
	}
	return res, nil
}
