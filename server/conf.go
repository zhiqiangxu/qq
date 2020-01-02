package server

import (
	"github.com/zhiqiangxu/qq"
	"github.com/zhiqiangxu/qrpc"
)

type (
	// Conf for QQ Server
	Conf struct {
		Broker          qq.BrokerConf
		Qrpc            qrpc.ServerBinding
		HTTP            HTTPConf
		PIDFile         string
		GracefulUpgrade bool
	}
	// HTTPConf for http server
	HTTPConf struct {
		Addr string
	}
)
