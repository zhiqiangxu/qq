package server

import (
	"sync"

	"github.com/zhiqiangxu/qq"
	"github.com/zhiqiangxu/qq/client/pb"
	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/util/logger"
	"go.uber.org/zap"
)

type pubCmd struct {
	BaseCmd
	broker *qq.Broker
}

// NewPubCmd is ctor for pubCmd
func NewPubCmd(broker *qq.Broker) qrpc.Handler {
	return &pubCmd{broker: broker}
}

var pubReqPool = sync.Pool{New: func() interface{} {
	return &pb.PubReq{}
}}

func (cmd *pubCmd) ServeQRPC(writer qrpc.FrameWriter, frame *qrpc.RequestFrame) {
	var (
		pubResp pb.PubResp
	)

	pubReq := pubReqPool.Get().(*pb.PubReq)
	defer func() {
		*pubReq = pb.PubReq{}
		pubReqPool.Put(pubReq)
	}()

	err := pubReq.Unmarshal(frame.Payload)
	if err != nil {
		logger.Instance().Error("pubCmd Unmarshal", zap.Error(err))
		pubResp.Code = qq.CodeUnmarshal
		bytes, _ /*always non nil for pb*/ := pubResp.Marshal()
		err = cmd.WriteRespBytes(writer, frame, qq.CmdPubResp, bytes, 0)
		if err != nil {
			logger.Instance().Error("pubCmd WriteRespBytes1", zap.Error(err))
		}
		return
	}

	pubResp = cmd.broker.Pub(pubReq)
	bytes, _ /*always non nil for pb*/ := pubResp.Marshal()
	err = cmd.WriteRespBytes(writer, frame, qq.CmdPubResp, bytes, 0)
	if err != nil {
		logger.Instance().Error("pubCmd WriteRespBytes2", zap.Error(err))
	}
}
