package server

import (
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

func (cmd *pubCmd) ServeQRPC(writer qrpc.FrameWriter, frame *qrpc.RequestFrame) {
	var (
		pubReq  pb.PubReq
		pubResp pb.PubResp
	)
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
