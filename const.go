package qq

import "github.com/zhiqiangxu/qrpc"

const (
	// CmdPub for publish
	CmdPub qrpc.Cmd = iota
	// CmdPubResp is resp for CmdPub
	CmdPubResp
	// CmdSub for subcribe
	CmdSub
	// CmdSubResp is resp for CmdSub
	CmdSubResp
)

const (
	// CodeOK for good
	CodeOK int32 = iota
	// CodeBrokerBlockWrite when broker is closing
	CodeBrokerBlockWrite
	// CodeUnmarshal for unmarshal error
	CodeUnmarshal
	// CodePutCommitLog for commit log Put error
	CodePutCommitLog
	// CodePutExchange for exchange Put error
	CodePutExchange
	// CodeNoSuchEx when no such exchange
	CodeNoSuchEx
)

const (
	// TypeTopic for direct topic
	TypeTopic int32 = iota
	// TypeExchange for indirect exchange
	TypeExchange
)

const (
	// ExchangeTypeDirect for direct exchange
	ExchangeTypeDirect int32 = iota
	// ExchangeTypeFanout for fanout exchange
	ExchangeTypeFanout
	// ExchangeTypeTopic for topic exchange
	ExchangeTypeTopic
)

const (
	// ConsumeQueueDataSize for consume queue data size
	ConsumeQueueDataSize = 8
)

const (
	// DefaultExchangeName is a Direct exchange
	DefaultExchangeName = "dex"
	storeSubPath        = "store"
	consumeQueueSubPath = "cq"
	commitLogSubPath    = "cl"

	// for db
	prefixForExchange         = "ex:%s"
	prefixForExchangeBindedCQ = "exbind:%s:%s"
)
