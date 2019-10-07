'use strict';

const ChainInterface = require('./chainInterface');
const Events = require('events');

async function buildChannel(id, title, asPublisher, chainInterface, options = {}) {
	if (id == null && title == null) {
		throw Error(`Netiher Channel id nor title provided.`);
	}

	if (id && parseInt(id) != id) {
		throw Error(`Channel id must be an integer.`);
	}

	const {messageTimeout = 20, skipDepth = 30} = options;

	let publisher = null;
	let staleTimeout = 100000;

	if (parseInt(id) >= 0) {
		const result = await chainInterface.getChannel(id, asPublisher);

		if (result.error) {
			throw Error(`Failed to connect: ${result.error}.`);
		}

		title = result.metaData.title;
		publisher = result.publisher;
	} else {
		asPublisher = true;
		const result = await chainInterface.createChannel({title});

		if (result.error) {
			return Error(`Failed to createChannel: ${result.error}.`);
		}

		id = result.id;
		publisher = result.publisher;
	}

	const messageActions = {
		publish: content => chainInterface.publishMessage(id, content),
		markRead: messageId => chainInterface.markMessageRead(id, messageId),
		skip: messageId => chainInterface.skipMessage(id, messageId)
	}

	return Object.freeze({
		title: title,

		id: id,

		publisher: publisher,

		publishMessage: function(content) {
			if (!asPublisher) {
				throw `Cannot publish as subscriber.`
			}

			return createMessage({content}, messageActions);
		},

		subscribe: function(options = {}) {
			const eventEmitter = new Events.EventEmitter();
			const emitEvent = () => eventEmitter.emit('messageReady');

			const pendingStaleEvents = {};
			const pendingSkippedEvents = {};

			const handledNew = messageId => {
				emitEvent();
			};

			const handledSkipped = messageId => {
				clearTimeout(pendingStaleEvents[messageId]);
				pendingSkippedEvents[messageId] = setTimeout(emitEvent, skipDepth*1000);
			};

			const handledPopped = messageId => {
				clearTimeout(pendingSkippedEvents[messageId]);
				pendingStaleEvents[messageId] = setTimeout(emitEvent, messageTimeout*1000);
			};

			const handledRead = messageId => {
				clearTimeout(pendingStaleEvents[messageId]);
			};

			chainInterface.subscribeToNew(id, handledNew);

			if (options.includeSkipped) {
				chainInterface.subscribeToSkipped(id, handledSkipped);
				chainInterface.subscribeToPopped(id, handledPopped);
			}

			if (options.includeStale) {
				chainInterface.subscribeToPopped(id, handledPopped);
				chainInterface.subscribeToSkipped(id, handledSkipped);
				chainInterface.subscribeToRead(id, handledRead);
			}

			return eventEmitter;
		},

		getNextMessage: async function() {
			if (asPublisher) {
				throw `Cannot consume as publisher.`;
			}

			const result = await chainInterface.getNextMessage(id);

			if (result.error) {
				throw Error(`Failed to get next message for channel ${id}: ${result.error}.`);
			}

			const {messageData} = result;

			if (!messageData) {
				return null;
			}

			return createMessage(messageData, messageActions);
		}
	});
}

async function createMessage(data, messageActions) {
	if (data.id && parseInt(data.id) != data.id) {
		throw Error(`Message id must be an integer.`);
	}

	if (data.id == null) {
		const result = await messageActions.publish(data.content);

		if (result.error) {
			throw Error(`Failed to publish: ${result.error}.`);
		}

		data.id = result.id;
	}

	return Object.freeze({
		id: data.id,

		data: data.content,

		markRead: async function() {
			const result = await messageActions.markRead(data.id);

			if (result.error) {
				throw Error(`Failed to mark as read: ${result.error}.`);
			}

			return true;
		},

		skip: async function() {
			const result = await messageActions.skip(data.id);

			if (result.error) {
				throw Error(`Failed to mark as read: ${result.error}.`);
			}
			
			return true;
		}
	});
}

module.exports = {
	createClient: function(nodeUrl, keyInfo, contractAddress, options = {}) {
		const chainInterface = new ChainInterface(nodeUrl, keyInfo, contractAddress, options);

		return Object.freeze({
			createChannel: function(title) {
				return buildChannel(null, title, true, chainInterface, options);
			},

			getChannel: function(id, asPublisher) {
				return buildChannel(id, null, asPublisher, chainInterface, options);
			}
		});
	},

	deployChannelManager: async function(nodeUrl, privateKey, chainId) {
		const {contractCode} = require('./config.json');
		const Web3 = require('web3');
		const web3 = new Web3(nodeUrl);
		const {address, signTransaction} = web3.eth.accounts.privateKeyToAccount(privateKey);
		const tx = {
			nonce: await web3.eth.getTransactionCount(address),
			chainId: chainId,
			data: contractCode,
			value: '0',
			gasPrice: '1',
			gas: 10000000
		};
		const signedTx = await signTransaction(tx);
		const receipt = await web3.eth.sendSignedTransaction(signedTx.rawTransaction);
		return receipt.contractAddress;
	}
}