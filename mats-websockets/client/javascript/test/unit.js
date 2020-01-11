const assert = require('assert');
const sinon = require('sinon');
const ws = require('ws');
const {MatsSocket} = require('../lib/MatsSocket');

describe('MatsSocket', function () {
    describe('constructor', function () {
        it('Should fail on no arg invocation', function () {
            assert.throws(() => new MatsSocket());
        });

        it('Should fail if appVersion and urls are missing', function () {
            assert.throws(() => new MatsSocket("Test"));
        });

        it('Should fail if urls are missing', function () {
            assert.throws(() => new MatsSocket("Test", "1.0"));
        });
        it('Should not invoke the WebSocket constructor', function () {
            const callback = sinon.spy(ws);
            new MatsSocket("Test", "1.0", ["ws://localhost:8080/"]);
            assert(!callback.called);
        });
    });

    describe('authorization', function () {
        const webSocket = {
            send(payload) {
                const [, message] = JSON.parse(payload);
                setTimeout(() => {
                    webSocket.onmessage({
                        data: JSON.stringify([
                            {t: "WELCOME"},
                            {t: "RECEIVED", cmseq: message.cmseq, st: "ACK"}
                        ])
                    });
                }, 0);
            }
        };
        Object.defineProperty(webSocket, "onopen", {
            set(callback) {
                setTimeout(() => callback({}), 0);
            }
        });

        it('Should invoke authorization callback before making calls', async function () {
            const matsSocket = new MatsSocket("Test", "1.0", ["ws://localhost:8080/"], () => webSocket);

            let authCallbackCalled = false;

            matsSocket.setAuthorizationExpiredCallback(function (event) {
                authCallbackCalled = true;
                matsSocket.setCurrentAuthorization("Test", Date.now() + 20000, 0);
            });
            await matsSocket.send("Test.authCallback", "SEND_" + matsSocket.id(6), {});

            assert(authCallbackCalled);
        });

        it('Should not invoke authorization callback if authorization present', async function () {
            const matsSocket = new MatsSocket("Test", "1.0", ["ws://localhost:8080/"], () => webSocket);

            let authCallbackCalled = false;
            matsSocket.setCurrentAuthorization("Test", Date.now() + 20000, 0);
            matsSocket.setAuthorizationExpiredCallback(function (event) {
                authCallbackCalled = true;
            });
            await matsSocket.send("Test.authCallback", "SEND_" + matsSocket.id(6), {});

            assert(!authCallbackCalled);
        });

        it('Should invoke authorization callback when expired', async function () {
            const matsSocket = new MatsSocket("Test", "1.0", ["ws://localhost:8080/"], () => webSocket);

            let authCallbackCalled = false;

            matsSocket.setCurrentAuthorization("Test", Date.now() - 20000, 0);
            matsSocket.setAuthorizationExpiredCallback(function (event) {
                authCallbackCalled = true;
                matsSocket.setCurrentAuthorization("Test", Date.now() + 20000, 0);
            });
            await matsSocket.send("Test.authCallback", "SEND_" + matsSocket.id(6), {});

            assert(authCallbackCalled);
        });

        it('Should invoke authorization callback when room for latency expired', async function () {
            const matsSocket = new MatsSocket("Test", "1.0", ["ws://localhost:8080/"], () => webSocket);

            let authCallbackCalled = false;

            matsSocket.setCurrentAuthorization("Test", Date.now() + 1000, 2000);
            matsSocket.setAuthorizationExpiredCallback(function (event) {
                authCallbackCalled = true;
                matsSocket.setCurrentAuthorization("Test", Date.now() + 20000, 0);
            });
            await matsSocket.send("Test.authCallback", "SEND_" + matsSocket.id(6), {});

            assert(authCallbackCalled);
        });
    });
});

