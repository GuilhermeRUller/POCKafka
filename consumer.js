global.config = {};
//config selector
switch (process.env.NODE_ENV) {
	//DEV ENVIRONMENT
	case 'dev':
	case 'development':
		config = require('./configs/config.devel');
		break;
	//HML ENVIRONMENT
	case 'hml':
	case 'homol':
		config = require('./configs/config.homol');
		break;
	//PROD ENVIRONMENT
	case 'prod':
	case 'production':
		config = require('./configs/config.stable');
	default:
		throw "Set environment ('dev', 'homol', 'prod')";
		break;
}

const { Kafka } = require('kafkajs');
const { SchemaRegistry }  = require("@kafkajs/confluent-schema-registry");

const kafka = new Kafka(config.kafka);
const registry = new SchemaRegistry(config.schemaRegistry);

const consumer = kafka.consumer({ groupId: 'Guilherme', rackId: '1' });

const run = async (strTopic) => {
	// Consuming
	await consumer.connect()
	await consumer.subscribe({ topic: strTopic, fromBeginning: true })
	console.log(`|*| START ${strTopic} consumer`)
	await consumer.run({
		partitionsConsumedConcurrently: 3,
		eachMessage: async ({ topic, partition, message }) => {
			try {
				console.log('|Message Partition + Offset + date received| ', partition, message.offset, new Date())
				console.log('|Key| ', (message.key ? message.key.toString() : ''))
				console.log('|Value| ', message.value.toString())
				if(message.key) {
					const decodedKey = await registry.decode(message.key)
					console.log('|decodedKey| ',decodedKey)
				}
				const decodedValue = await registry.decode(message.value)
				console.log('|decodedValue| ',decodedValue)
				console.log('|message| ',message)
			} catch(e) {
				console.log('|Error| ', e)
			}
			console.log('\n\n\n\n')
		},
	});
}

run('ingress-updated').catch(console.error);
