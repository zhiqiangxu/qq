package qq

import (
	"github.com/zhiqiangxu/qq/client/pb"
	"github.com/zhiqiangxu/util/logger"
	"go.uber.org/zap"
)

type exchange interface {
	Put(offset int64, req pb.PubReq) error
}

var _ exchange = (*Exchange)(nil)

// Exchange for qq
type Exchange struct {
	exType int32

	putFunc func(offset int64, req pb.PubReq) error
}

// NewExchange is ctor for Exchange
func NewExchange(exType int32) (ex *Exchange) {
	ex = &Exchange{exType: exType}
	ex.init()
	return
}

func (ex *Exchange) init() {
	switch ex.exType {
	case ExchangeTypeDirect:
		ex.putFunc = ex.putDirect
	case ExchangeTypeFanout:
		ex.putFunc = ex.putFanout
	case ExchangeTypeTopic:
		ex.putFunc = ex.putTopic
	default:
		logger.Instance().Fatal("invalid exType", zap.Int32("exType", ex.exType))
	}
}

// Put to child queue(s)
func (ex *Exchange) Put(offset int64, req pb.PubReq) error {
	return ex.putFunc(offset, req)
}

func (ex *Exchange) putDirect(offset int64, req pb.PubReq) error {
	return nil
}

func (ex *Exchange) putFanout(offset int64, req pb.PubReq) error {
	return nil
}

func (ex *Exchange) putTopic(offset int64, req pb.PubReq) error {
	return nil
}
