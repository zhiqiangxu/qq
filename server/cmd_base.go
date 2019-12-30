package server

import "github.com/zhiqiangxu/qrpc"

// BaseCmd for common behaviour
type BaseCmd struct {
}

// WriteRespBytes for write resp bytes
func (cmd BaseCmd) WriteRespBytes(writer qrpc.FrameWriter, frame *qrpc.RequestFrame, respCmd qrpc.Cmd, bytes []byte, flags qrpc.FrameFlag) (err error) {
	writer.StartWrite(frame.RequestID, respCmd, flags)
	writer.WriteBytes(bytes)

	err = writer.EndWrite()

	return nil
}
