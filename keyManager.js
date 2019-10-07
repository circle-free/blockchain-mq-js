function KeyManager(xpriv, hdindex) {
	return {
		privatekey: 'derivedPrivateKey',
		address: 'derivedAddress'
	};
}

module.exports = KeyManager;