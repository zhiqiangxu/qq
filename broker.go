package qq

import (
	"github.com/zhiqiangxu/qq/client/pb"
	"github.com/zhiqiangxu/qrpc"
)

type broker interface {
	Pub(req pb.PubReq) pb.PubResp
	Sub(req pb.SubReq, ci *qrpc.ConnectionInfo) pb.SubResp
}

// Broker for qq
type Broker struct {
	cl   *CommitLog
	mm   *MetaManager
	conf BrokerConf
}

// NewBroker is ctor for Broker
func NewBroker(conf BrokerConf) *Broker {
	return &Broker{conf: conf}
}

// Pub for publish
func (b *Broker) Pub(req pb.PubReq) (resp pb.PubResp) {

	offset, err := b.cl.Put(req.Data)
	if err != nil {
		resp.Code = CodePutCommitLog
		resp.Msg = err.Error()
		return
	}

	// Put offset into consume queue
	ex := b.mm.GetExchange(req.Exchange)
	if ex == nil {
		resp.Code = CodeNoSuchEx
		return
	}

	err = ex.Put(offset, req)
	if err != nil {
		resp.Code = CodePutExchange
		resp.Msg = err.Error()
		return
	}

	return
}

// Sub for subscribe
func (b *Broker) Sub(req pb.SubReq, ci *qrpc.ConnectionInfo) (resp pb.SubResp) {
	return
}
