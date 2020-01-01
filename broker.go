package qq

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/zhiqiangxu/qq/client/pb"
	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/util"
)

type broker interface {
	Pub(req *pb.PubReq) pb.PubResp
	Sub(req pb.SubReq, ci *qrpc.ConnectionInfo) pb.SubResp
	Close()
}

// Broker for qq
type Broker struct {
	closeOnce  sync.Once
	closeState uint32
	cl         *CommitLog
	mm         *MetaManager
	conf       BrokerConf
	closer     *util.Closer
}

// NewBroker is ctor for Broker
func NewBroker(conf BrokerConf) *Broker {
	b := &Broker{conf: conf, closer: util.NewCloser()}
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

func (b *Broker) checkCloseState() (err error) {
	closeState := atomic.LoadUint32(&b.closeState)
	switch closeState {
	case open:
	case closing:
		err = errAlreadyClosing
	case closed:
		err = errAlreadyClosed
	default:
		err = fmt.Errorf("unknown closeState:%d", closeState)
	}
	return
}

// Pub for publish
func (b *Broker) Pub(req *pb.PubReq) (resp pb.PubResp) {

	err := b.checkCloseState()
	if err != nil {
		resp.Code = CodeBrokerBlockWrite
		return
	}

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
		atomic.StoreUint32(&b.closeState, closing)

		b.closer.SignalAndWait()

		atomic.StoreUint32(&b.closeState, closed)
	})
}
