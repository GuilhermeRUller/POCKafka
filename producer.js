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
	var buf = new Buffer.alloc(length || 4024);
	buf[0] = 0; // Magic byte.
	buf.writeInt32BE(schemaId, 1);

	var pos = type.encode(val, buf, 5);
	if (pos < 0) {
		// The buffer was too short, we need to resize.
		throw "The buffer was too short, we need to resize.";
	}
	return buf.slice(0, pos);
}

const run = async (strTopic, boolResend=false) => {
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
	var bufValue = toMessageBuffer(objMessage, typeValue, idValue); // Assuming 1 is your schema's ID.
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
			run(strTopic, boolResend).catch(console.error);
		}, 1000);
	}
}

const objKey = {id: '00000000000'};
const objMessage = {
	system: 'teste',
	account: {
		businessKey: '00000000000',
		businessKeyType: 'BR_CPF',
		recordType: 'ADMINISTRATIVO',
		recordGroup: 'CLIENTE',
		name: 'aaaa de aaaa',
		parent: null
	},
	contact: {
		businessKey: '00000000000',
		businessKeyType: 'BR_CPF',
		recordType: 'PESSOA_FISICA',
		fullName: 'aaaa de aaaa',
		birthDate: -637,
		documentNumber: '00000000000',
		mobilePhone: null,
		homePhone: '+5500000000000',
		phone: '+5500000000000',
		maritalStatus: null,
		hasOptedOutOfEmail: false,
		hasOptedOutOfSms: false,
		hasOptedOutOfWhatsApp: false,
		alternateEmail: 'aaaa@hotmail.com',
		gender: 'NAO_INFORMADO',
		rg: '',
		preferredEmail: '',
		preferredPhone: '+5500000000000',
		ethnicity: null,
		race: null,
		religion: null,
		workEmail: '',
		workPhone: '+5500000000000',
		chosenFullName: null,
		citizenship: null,
		countryOfOrigin: null,
		deceased: null,
		doNotContact: false,
		dualCitizenship: null,
		financialAidApplicant: null,
		specialNeeds: null
	},
	address: {
		addressType: 'Residencial',
		defaultAddress: null,
		latestEndDate: null,
		latestStartDate: null,
		mailingCity: 'Botafogo',
		mailingCountry: 'Brasil',
		mailingPostalCode: '00000000',
		mailingState: 'RJ',
		mailingStreet: 'Rua Voluntarios da Patria',
		district: 'Botafogo',
		number: '000',
		complement: 'dado fixo',
		ibgeCode: '3304557',
		seasonalEndDay: null,
		seasonalEndMonth: null,
		seasonalEndYear: null,
		seasonalStartDay: null,
		seasonalStartMonth: null,
		seasonalStartYear: null
	},
	affiliation: {
		newBusinessKeyObject: null,
		businessKey: 'CPF_00000000000_OFFER_0000000-00000000-0000000-00000-0000000-0-00_TIMESTAMP_1644795087',
		businessKeyType: 'CPF_OFERTA',
		status: 'AGUARDANDO_RESULTADO_SELETIVO',
		userEnem: false,
		highSchoolCompletion: 0,
		ownerId: '00000000000',
		businessKeyOffer: '0000000-00000000-0000000-00000-0000000-0-00',
		idSalesChannel: '70',
		examType: 'PRESENCIAL',
		businessKeyExam: '00000000-0000-00e0-aad0-f0d000000000',
		businessKeyTypeAccountOffer: 'BR_CNPJ',
		businessKeyAccountOffer: '00000000000000'
	}
};

run('ingress-updated', process.env.RESEND).catch(console.error);
