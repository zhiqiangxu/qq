package main

import (
	"os"

	"github.com/zhiqiangxu/qq"
	"github.com/zhiqiangxu/qq/server"
	"github.com/zhiqiangxu/qrpc"
)

func main() {

	os.MkdirAll("/tmp/qq", 0770)

	conf := server.Conf{
		Broker:  qq.BrokerConf{DataDir: "/tmp/qq/broker"},
		Qrpc:    qrpc.ServerBinding{Addr: ":8888"},
		HTTP:    server.HTTPConf{Addr: ":8080"},
		PIDFile: "/tmp/qq/pid",
	}
	qq := server.NewQQ(conf)

	qq.Start()
}
