package server

import (
	"github.com/zhiqiangxu/qq"
	"github.com/zhiqiangxu/qq/client/pb"
	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/util/logger"
	"go.uber.org/zap"
)

type subCmd struct {
	BaseCmd
	broker *qq.Broker
}

// NewSubCmd is ctor for subCmd
func NewSubCmd(broker *qq.Broker) qrpc.Handler {
	return &subCmd{broker: broker}
}

func (cmd *subCmd) ServeQRPC(writer qrpc.FrameWriter, frame *qrpc.RequestFrame) {
	var (
		subReq  pb.SubReq
		subResp pb.SubResp
	)
	err := subReq.Unmarshal(frame.Payload)
	if err != nil {
		logger.Instance().Error("subCmd Unmarshal", zap.Error(err))
		subResp.Code = qq.CodeUnmarshal
		bytes, _ /*always non nil for pb*/ := subResp.Marshal()
		err = cmd.WriteRespBytes(writer, frame, qq.CmdSubResp, bytes, 0)
		if err != nil {
			logger.Instance().Error("subCmd WriteRespBytes1", zap.Error(err))
		}
		return
	}

	subResp = cmd.broker.Sub(&subReq, frame.ConnectionInfo())
	bytes, _ /*always non nil for pb*/ := subResp.Marshal()
	err = cmd.WriteRespBytes(writer, frame, qq.CmdSubResp, bytes, 0)
	if err != nil {
		logger.Instance().Error("subCmd WriteRespBytes2", zap.Error(err))
	}
}
