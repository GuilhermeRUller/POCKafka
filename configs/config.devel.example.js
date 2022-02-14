var config            = {};
config.env            = 'DEVEL';
config.version        = 'V1.0';

config.kafka          = {
		brokers: ["brokerHost1","brokerHost2"],
		ssl: true,
		sasl: {
			mechanism: "plain",
			username: "brokerKey",
			password: "brokerSecret",
		}
	};

config.schemaRegistry = {
		host: "schemaRegistryHost",
		auth: {
			username: "schemaRegistryKey",
			password: "schemaRegistrySecret",
		}
	};

module.exports        = config;