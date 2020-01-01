package qq

import (
	"errors"
	"sync"

	"github.com/zhiqiangxu/qq/client/pb"
	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/util/closer"
)

type broker interface {
	Pub(req *pb.PubReq) pb.PubResp
	Sub(req pb.SubReq, ci *qrpc.ConnectionInfo) pb.SubResp
	Close()
}

// Broker for qq
type Broker struct {
	closeOnce sync.Once
	cl        *CommitLog
	mm        *MetaManager
	conf      BrokerConf
	closer    closer.State
}

// NewBroker is ctor for Broker
func NewBroker(conf BrokerConf) *Broker {
	b := &Broker{conf: conf}
	b.cl = NewCommitLog(b)
	b.mm = NewMetaManager(b)

	return b
}

const (
	open uint32 = iota
	closing
	closed
)

var (
	errAlreadyClosed  = errors.New("already closed")
	errAlreadyClosing = errors.New("already closing")
)

// Pub for publish
func (b *Broker) Pub(req *pb.PubReq) (resp pb.PubResp) {

	b.closer.Add(1)
	defer b.closer.Done()

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

// Close broker
func (b *Broker) Close() {
	b.closeOnce.Do(func() {

		b.closer.SignalAndWait()

	})
}
