/**
 * 
 * Criar o tópico 
 * 
 * 
 * 
 **/
const { Kafka } = require('kafkajs');

iCount = 0;

const kafka = new Kafka({
		// clientId: 'my-app',
		// brokers: ['kafka1:9092', 'kafka2:9092']
		brokers: ['localhost:9092']
	});

const producer = kafka.producer();

const run = async () => {
	// Producing
	await producer.connect();

	await producer.send({
			topic: 'Teste_123',
			messages: [
				// { value: JSON.stringify({ message: 'Hello KafkaJS user!', datetime: new Date()}) }
				{ value: 'NodeJS this is message ' + iCount }
			],
		});
	console.log('Sended message', iCount);
	iCount++;

	setTimeout(function () {
		// execute script
		run().catch(console.error);
	}, 1000);
}

run().catch(console.error);