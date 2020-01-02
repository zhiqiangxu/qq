package server

import (
	"net/http"
	"os"
	"syscall"

	"os/signal"

	"github.com/cloudflare/tableflip"
	"github.com/oklog/run"
	"github.com/zhiqiangxu/qq"
	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/util/logger"
	"go.uber.org/zap"
)

// QQ server
type QQ struct {
	conf Conf
}

// NewQQ is ctor for QQ
func NewQQ(conf Conf) *QQ {
	return &QQ{conf: conf}
}

var ver string

// Start QQ server
func (s *QQ) Start() (err error) {
	broker := qq.NewBroker(s.conf.Broker)
	mux := qrpc.NewServeMux()
	mux.Handle(qq.CmdPub, NewPubCmd(broker))
	mux.Handle(qq.CmdSub, NewSubCmd(broker))

	upg, err := tableflip.New(tableflip.Options{PIDFile: s.conf.PIDFile})
	if err != nil {
		logger.Instance().Fatal("tableflip.New", zap.Error(err))
		return
	}
	defer upg.Stop()

	s.conf.Qrpc.ListenFunc = upg.Fds.Listen
	qserver := qrpc.NewServer([]qrpc.ServerBinding{s.conf.Qrpc})
	err = qserver.ListenAll()
	if err != nil {
		logger.Instance().Fatal("qserver.ListenAll", zap.Error(err))
	}

	httpServer := &http.Server{Addr: s.conf.HTTP.Addr}
	ln, err := upg.Fds.Listen("tcp", s.conf.HTTP.Addr)
	if err != nil {
		logger.Instance().Fatal("upg.Fds.Listen", zap.String("http addr", s.conf.HTTP.Addr), zap.Error(err))
	}

	var g run.Group
	g.Add(func() error {
		err := qserver.ServeAll()
		if err != nil {
			logger.Instance().Error("ServeAll", zap.Error(err))
		}
		return err
	}, func(err error) {
		shutdownQRPC(qserver)
		logger.Instance().Info("shutdownQRPC", zap.Error(err))
	})

	g.Add(func() error {
		err = httpServer.Serve(ln)
		if err != nil {
			logger.Instance().Error("httpServer.Serve", zap.Error(err))
		}
		return err
	}, func(err error) {
		shutdownHTTP(httpServer)
		logger.Instance().Info("shutdownHTTP", zap.Error(err))
	})

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGTERM, syscall.SIGUSR1, syscall.SIGQUIT)
		for range sig {
			// upgrade on signal
			logger.Instance().Error("conf", zap.Bool("GracefulUpgrade", s.conf.GracefulUpgrade))

			broker.Close()
			logger.Instance().Error("broker stopped")

			if !s.conf.GracefulUpgrade {
				shutdownQRPC(qserver)
				shutdownHTTP(httpServer)
				os.Exit(1)
			}
			logger.Instance().Error("upgrade start")
			err := upg.Upgrade()
			logger.Instance().Error("upgrade end", zap.Error(err))
			if err != nil {
				logger.Instance().Error("upgrade failed", zap.Error(err))
			}
		}

	}()

	groupDoneCh := make(chan struct{})
	go func() {
		err := g.Run()
		if err != nil {
			logger.Instance().Error("Group.Run", zap.Error(err))
		}
		close(groupDoneCh)
	}()

	if err = upg.Ready(); err != nil {
		logger.Instance().Error("upg.Ready", zap.Error(err))
		return
	}

	logger.Instance().Error("child ready to serve", zap.String("ver", ver))

	// ready to exit
	select {
	case <-upg.Exit():
	case <-groupDoneCh:
	}
	logger.Instance().Error("parent prepare for exit", zap.String("ver", ver))

	shutdownQRPC(qserver)
	shutdownHTTP(httpServer)

	logger.Instance().Error("parent quit ok")
	return
}

func shutdownQRPC(qserver *qrpc.Server) {
	logger.Instance().Error("before qserver.Shutdown", zap.String("ver", ver))
	err := qserver.Shutdown()
	logger.Instance().Error("after qserver.Shutdown", zap.String("ver", ver))
	if err != nil {
		logger.Instance().Error("qserver.Shutdown", zap.String("ver", ver), zap.Error(err))
	}
}

func shutdownHTTP(httpServer *http.Server) {
	err := httpServer.Close()
	logger.Instance().Error("httpServer.Close")
	if err != nil {
		logger.Instance().Error("httpServer.Close", zap.Error(err))
	}
}
