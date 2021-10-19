const { Kafka } = require('kafkajs');

const kafka = new Kafka({
		// clientId: 'my-app',
		// brokers: ['kafka1:9092', 'kafka2:9092']
		brokers: ['localhost:9092']
	});

const consumer = kafka.consumer({ groupId: 'my-app', rackId: '1' });
iCount=0;
const run = async () => {
	// Consuming
	await consumer.connect()
	await consumer.subscribe({ topic: 'Teste_123', fromBeginning: true })

	await consumer.run({
		partitionsConsumedConcurrently: 3,
		eachMessage: async ({ topic, partition, message }) => {
			console.log(JSON.stringify({
				partition,
				offset: message.offset,
				value: message.value.toString(),
				datetime: new Date()
			}));
			iCount++;
			if(iCount === 1){
				console.log('START '+JSON.stringify(new Date()))
			}
			if(iCount === 10000){
				console.log('STOP '+JSON.stringify(new Date()))
			}
		},
	});
}

run().catch(console.error);