'use strict';

const EventEmitter = require('events');
const KeyManager = require('./keyManager');
const Web3 = require('web3');
const solc = require('solc');
const {contractABI, contractAddress} = require('./config.json');

// const eventNames = contractABI.filter(abiItem => abiItem.type === 'event').map(abiItem => abiItem.name);

const derivePath = (account, index) => `m/${account}'/${index}`;

const add0xPrefix = input => {
	if (input.startsWith('0x')) {
		return input;
	}

	return `0x${input}`;
};

const remove0xPrefix = input => {
	if (!input.startsWith('0x')) {
		return input;
	}

	return input.substr(2);
};

// TODO: consider using Web3.utils.stringToHex
const encode = data => add0xPrefix(Buffer(JSON.stringify(data)).toString('hex'));

// TODO: consider using Web3.utils.hexToString
const decode = data => JSON.parse(Buffer(remove0xPrefix(data), 'hex').toString());

const buildTx = (chainId, contractAddress, nonce, data)  => {
	return {
		nonce: nonce,
		chainId: chainId,
		to: contractAddress,
		data: data,
		value: '0',
		gasPrice: '1',
		gas: 1000000000
	};
};

const getEventABI = eventName => contractABI.find(elem => elem.name === eventName && elem.type === 'event');

const extractLog = (receipt, eventABI) => receipt.logs.find(log => log.topics[0] === eventABI.signature);

const decodeLog = (web3, eventABI, log) => web3.eth.abi.decodeLog(eventABI.inputs, log.data, log.topics);

const extractDecodedLog = (web3, receipt, eventName) => {
	const eventABI = getEventABI(eventName);
	return decodeLog(web3, eventABI, extractLog(receipt, eventABI))
};

const objectToByteCode = json => {
	let codeString = 'contract genericFlatMap { mapping (bytes32 => bytes) private properties;';

	const writeProperties = (obj, path) => {
		if (!obj) {
			return;
		}

		if (typeof obj !== 'object') {
			const objectKey = Web3.utils.keccak256(path);
			codeString = codeString.concat(`properties[${objectKey}] = ${obj};`);
			return;
		}

		for (let k in obj) {
			writeProperties(obj[k], path ? `${path}.${k}` : k);
		}
	};

	codeString = codeString.concat('function get(string propertyPath) external view returns (bytes) { return properties[keccak256(propertyPath)]; } })';

	let outputs = solc.compile(data, 1);

	return outputs.contracts['genericFlatMap'].bytecode;
};

class ChainInterface{
	constructor(url, keyInfo, contractAddress, options = {}) {
		if (!contractAddress) {
			throw `No contract address specified`;
		}

		const {chainId = 5777, messageTimeout = 5, skipDepth = 30, numOfRetries = 3} = options;

		const web3 = new Web3(url);
		let privateKey = null;

		if (keyInfo.xpriv) {
			// set keyInfo.xpub
			const hdChild = this.hdKey.derive(derivePath(keyInfo.account, keyInfo.hdIndex));
			privateKey = add0xPrefix(hdChild.privateKey.toString('hex'));
		} else if (keyInfo.privateKey) {
			privateKey = add0xPrefix(keyInfo.privateKey);
		}

		const {address, signTransaction, sign} = web3.eth.accounts.privateKeyToAccount(privateKey);

		let groupId = Web3.utils.keccak256(Web3.utils.stringToHex(keyInfo.xpub));

		let nonce = -1;

		const getNonce = async () => {
			if (nonce === -1) {
				nonce = await web3.eth.getTransactionCount(address);
				return nonce;
			} else {
				return ++nonce;
			}
		};

		const mqContract = new web3.eth.Contract(contractABI, contractAddress);
		const eventEmitters = {};

		mqContract.events.allEvents()
		.on('data', eventData => {
			if (eventData.event == "ChannelCreated" || eventData.event == "ConsumerJoined") {
				return;
			}

			if (eventData.returnValues.consumerGroupId && eventData.returnValues.consumerGroupId != groupId) {
				return;
			}

			if (!eventEmitters[eventData.returnValues.channelId]) {
				return;
			}

			eventEmitters[eventData.returnValues.channelId].emit(eventData.event, eventData.returnValues.messageId);
		})
		.on('error', console.error);

		this.getChannel = async function(channelId, asPublisher) {
			try {
				if (asPublisher && !(await mqContract.methods.isPublisher(channelId, address).call())) {
					const EVENT_NAME = 'PublisherJoined';
					const data = mqContract.methods.join(channelId, 0).encodeABI();
					const tx = buildTx(chainId, contractAddress, await getNonce(), data);
					const {messageHash, rawTransaction} = await signTransaction(tx);
					const receipt = await web3.eth.sendSignedTransaction(rawTransaction);

					if (!receipt.status) {
						throw `Jpoin transaction failed.`;
					}

					const log = extractDecodedLog(web3, receipt, EVENT_NAME);

					if (!log) {
						throw `No log for joining channel.`;
					}

					if (log.channelId != channelId || log.publisher != address) {
						console.error(`join: Expected channel ${channelId} and address ${address}, got ${log.channelId} and ${log.publisher}`);
						throw `Channel join log mismatch.`;
					}
				}

				if (!asPublisher && !(await mqContract.methods.isSubscriber(channelId, address).call())) {
					const EVENT_NAME = 'ConsumerJoined';
					// TODO: convert xpub to bytes
					const data = mqContract.methods.subscribe(channelId, Web3.utils.stringToHex(keyInfo.xpub), 0, messageTimeout, numOfRetries).encodeABI();
					const tx = buildTx(chainId, contractAddress, await getNonce(), data);
					const {messageHash, rawTransaction} = await signTransaction(tx);
					const receipt = await web3.eth.sendSignedTransaction(rawTransaction);

					if (!receipt.status) {
						throw `Subscribe transaction failed.`;
					}

					const log = extractDecodedLog(web3, receipt, EVENT_NAME);

					if (!log) {
						throw `No log for joining channel.`;
					}

					if (log.channelId != channelId || log.subscriber != address) {
						console.error(`subscribe: Expected channel ${channelId} and address ${address}, got ${log.channelId} and ${log.subscriber}`);
						throw `Channel join log mismatch.`;
					}
				}

				const channelMetadata = await mqContract.methods.getChannelMetadata(channelId).call();

				// TODO: publisher should be xpub later

				return {
					error: null,
					metaData: decode(channelMetadata)
				};
			} catch (error) {
				return {
					error: error
				};
			}
		};

		this.createChannel = async function(metaData) {
			try {
				const EVENT_NAME = 'ChannelCreated';
				const data = mqContract.methods.createChannel(Web3.utils.stringToHex(keyInfo.xpub), 0, encode(metaData)).encodeABI();
				const tx = buildTx(chainId, contractAddress, await getNonce(), data);
				const {messageHash, rawTransaction} = await signTransaction(tx);
				const receipt = await web3.eth.sendSignedTransaction(rawTransaction);

				if (!receipt.status) {
					return {
						error: 'Create transaction failed.'
					};
				}

				const log = extractDecodedLog(web3, receipt, EVENT_NAME);

				if (!log) {
					throw `No log for creating channel.`;
				}

				if (log.publisher != address) {
					console.error(`createChannel: Expected address ${address}, got ${log.publisher}`);
					throw `Channel create log mismatch.`;
				}

				// TODO: publisher should be xpub later

				return {
					error: null,
					id: log.channelId,
					publisher: log.publisher
				};
			} catch (error) {
				return {
					error: error
				};
			}
		};

		this.getNextMessage = async function(channelId) {
			try {
				const EVENT_NAME = 'MessagePopped';
				const data = mqContract.methods.getNext(channelId, skipDepth).encodeABI();
				const tx = buildTx(chainId, contractAddress, await getNonce(), data);
				const {messageHash, rawTransaction} = await signTransaction(tx);
				const receipt = await web3.eth.sendSignedTransaction(rawTransaction);

				if (!receipt.status) {
					// TODO: check.receipt.error for potetial error case, like no more messages
					return {
						error: 'Get Next transaction failed.'
					};
				}

				const log = extractDecodedLog(web3, receipt, EVENT_NAME);

				if (!log) {
					throw `No log for get next message.`;
				}

				if (log.channelId != channelId || log.subscriber != address) {
					console.error(`getNext: Expected channel ${channelId} and address ${address}, got ${log.channelId} and ${log.subscriber}`);
					throw `Get next message log mismatch.`;
				}

				const encodedMessage = await mqContract.methods.getMessage(channelId, log.messageId).call();

				return {
					error: null,
					messageData: {
						id: log.messageId,
						content: decode(encodedMessage)
					}
				};
			} catch (error) {
				if (error.toString().includes('revert')) {
					return {
						error: null,
						messageData: null
					};
				}

				return {
					error: error
				};
			}
		};

		this.publishMessage = async function(channelId, message) {
			try {
				const EVENT_NAME = 'MessagePublished';
				const data = mqContract.methods.publish(channelId, encode(message)).encodeABI();
				const tx = buildTx(chainId, contractAddress, await getNonce(), data);
				const {messageHash, rawTransaction} = await signTransaction(tx);
				const receipt = await web3.eth.sendSignedTransaction(rawTransaction);

				if (!receipt.status) {
					return {
						error: 'Publish transaction failed.'
					};
				}

				const log = extractDecodedLog(web3, receipt, EVENT_NAME);

				if (!log) {
					throw `No log for publish.`;
				}

				if (log.channelId != channelId || log.publisher != address) {
					console.error(`publish: Expected channel ${channelId} and address ${address}, got ${log.channelId} and ${log.publisher}`);
					throw `Publish log mismatch.`;
				}

				return {
					error: null,
					id: log.messageId
				};
			} catch (error) {
				return {
					error: error
				};
			}
		};

		this.markMessageRead = async function(channelId, messageId) {
			try {
				const EVENT_NAME = 'MessageRead';
				const data = mqContract.methods.confirm(channelId, messageId).encodeABI();
				const tx = buildTx(chainId, contractAddress, await getNonce(), data);
				const {messageHash, rawTransaction} = await signTransaction(tx);
				const receipt = await web3.eth.sendSignedTransaction(rawTransaction);

				if (!receipt.status) {
					return {
						error: 'Confirm transaction failed.'
					};
				}

				const log = extractDecodedLog(web3, receipt, EVENT_NAME);

				if (!log) {
					throw `No log for confirm message.`;
				}

				if (log.channelId != channelId || log.subscriber != address || log.messageId != messageId) {
					console.error(`confirm: Expected channel ${channelId}, address ${address}, and message ${messageId}, got ${log.channelId}, ${log.subscriber}, and ${log.messageId}`);
					throw `Confirm message log mismatch.`;
				}

				return {};
			} catch (error) {
				return {
					error: error
				};
			}
		};

		this.skipMessage = async function(channelId, messageId) {
			try {
				const EVENT_NAME = 'MessageSkipped';
				const data = mqContract.methods.skip(channelId, messageId).encodeABI();
				const tx = buildTx(chainId, contractAddress, await getNonce(), data);
				const {messageHash, rawTransaction} = await signTransaction(tx);
				const receipt = await web3.eth.sendSignedTransaction(rawTransaction);

				if (!receipt.status) {
					return {
						error: 'Skip transaction failed.'
					};
				}

				const log = extractDecodedLog(web3, receipt, EVENT_NAME);

				if (!log) {
					throw `No log for skip message.`;
				}

				if (log.channelId != channelId || log.subscriber != address || log.messageId != messageId) {
					console.error(`skip: Expected channel ${channelId}, address ${address}, and message ${messageId}, got ${log.channelId}, ${log.subscriber}, and ${log.messageId}`);
					throw `Skip message log mismatch.`;
				}

				return {};
			} catch (error) {
				return {
					error: error
				};
			}
		};

		this.subscribeToNew = function(channelId, callback) {
			if (!eventEmitters[channelId]) {
				eventEmitters[channelId] = new EventEmitter();
			}

			eventEmitters[channelId].on('MessagePublished', callback);
		};

		this.unsubscribeFromNew = function(channelId, callback) {
			if (!eventEmitters[channelId]) {
				eventEmitters[channelId] = new EventEmitter();
			}

			eventEmitters[channelId].off('MessagePublished', callback);
		};

		this.subscribeToSkipped = function(channelId, callback) {
			if (!eventEmitters[channelId]) {
				eventEmitters[channelId] = new EventEmitter();
			}

			eventEmitters[channelId].on('MessageSkipped', callback);
		};

		this.unsubscribeFromSkipped = function(channelId, callback) {
			if (!eventEmitters[channelId]) {
				eventEmitters[channelId] = new EventEmitter();
			}

			eventEmitters[channelId].off('MessageSkipped', callback);
		};

		this.subscribeToPopped = function(channelId, callback) {
			if (!eventEmitters[channelId]) {
				eventEmitters[channelId] = new EventEmitter();
			}

			eventEmitters[channelId].on('MessagePopped', callback);
		};

		this.unsubscribeFromPopped = function(channelId, callback) {
			if (!eventEmitters[channelId]) {
				eventEmitters[channelId] = new EventEmitter();
			}

			eventEmitters[channelId].off('MessagePopped', callback);
		};

		this.subscribeToRead = function(channelId, callback) {
			if (!eventEmitters[channelId]) {
				eventEmitters[channelId] = new EventEmitter();
			}

			eventEmitters[channelId].on('MessageRead', callback);
		};

		this.unsubscribeFromRead = function(channelId, callback) {
			if (!eventEmitters[channelId]) {
				eventEmitters[channelId] = new EventEmitter();
			}

			eventEmitters[channelId].off('MessageRead', callback);
		};
	}
}

module.exports = ChainInterface;
