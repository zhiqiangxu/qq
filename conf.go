package qq

type (
	// Conf for qq
	Conf struct {
		ServerConf ServerConf
		BrokerConf BrokerConf
	}

	// ServerConf is conf for server
	ServerConf struct {
		ListenAddr string
	}
	// BrokerConf is conf for broker
	BrokerConf struct {
		DataDir string
	}
)
