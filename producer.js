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

const { Kafka, CompressionTypes } = require('kafkajs');

const { SchemaRegistry }  = require("@kafkajs/confluent-schema-registry");
const avro = require('avsc');

const kafka = new Kafka(config.kafka);
const registry = new SchemaRegistry(config.schemaRegistry);

const producer = kafka.producer();


/**
 * Encode an Avro value into a message, as expected by Confluent's Kafka Avro
 * deserializer.
 *
 * @param val {...} The Avro value to encode.
 * @param type {Type} Your value's Avro type.
 * @param schemaId {Integer} Your schema's ID (inside the registry).
 * @param length {Integer} Optional initial buffer length. Set it high enough
 * to avoid having to resize. Defaults to 1024.
 *
 */
function toMessageBuffer(val, type, schemaId, length) {
	console.log(length)
	var buf = new Buffer.alloc(length || 500024);
	buf[0] = 0; // Magic byte.
	buf.writeInt32BE(schemaId, 1);

	var pos = type.encode(val, buf, 5);
	if (pos < 0) {
		// The buffer was too short, we need to resize.
		throw "The buffer was too short, we need to resize.";
	}
	return buf.slice(0, pos);
}

const run = async (strFile, boolResend=false) => {
	let obj = require('./payloads/'+ strFile);



	const strTopic = obj.strTopic;
	const objKey = obj.objKey;
	const objMessage = obj.objMessage;


	const subjectKey = strTopic + "-key";
	const subjectValue = strTopic + "-value";

	// get schema ID from schema registry
	const idKey = await registry.getLatestSchemaId(subjectKey);
	// get schema from schema registry
	let testeKey = await registry.getSchema(idKey)
	// create new Type from schema
	const typeKey = await avro.Type.forSchema(testeKey);

	// get schema ID from schema registry
	const idValue = await registry.getLatestSchemaId(subjectValue);
	// get schema from schema registry
	let testeValue = await registry.getSchema(idValue)
	// create new Type from schema
	const typeValue = await avro.Type.forSchema(testeValue);

	// const bufKey = await typeKey.toBuffer(objKey); // Encoded buffer.
	const bufKey = await toMessageBuffer(objKey, typeKey,idKey); // Encoded buffer.
	// const valKey = await typeKey.fromBuffer(bufKey);

	// const bufValue = await typeValue.toBuffer(objMessage); // Encoded buffer.
	var bufValue = toMessageBuffer(objMessage, typeValue, idValue, (JSON.stringify(objMessage).length + 1000)); // Assuming 1 is your schema's ID.
	// const valValue = await typeValue.fromBuffer(bufValue);

	// Producing
	await producer.connect();
	let result = await producer.send({
			topic: strTopic,
			compression: CompressionTypes.GZIP,
			messages: [
					{ key: bufKey,value: bufValue }
				// { value: bufValue }
			],
		});

	console.log('Sended message', JSON.stringify(result));

	if (boolResend){
		setTimeout(function () {
			// execute script
			run(strFile, boolResend).catch(console.error);
		}, 1000);
	}
}


run(process.env.FILE, process.env.RESEND).catch(console.error);
