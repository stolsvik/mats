// Register as an UMD module - source: https://github.com/umdjs/umd/blob/master/templates/commonjsStrict.js
(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        // AMD. Register as an anonymous module.
        define(['chai', 'sinon', 'ws', 'mats'], factory);
    } else if (typeof exports === 'object' && typeof exports.nodeName !== 'string') {
        // CommonJS
        const chai = require('chai');
        const sinon = require('sinon');
        const ws = require('ws');
        const mats = require('../lib/MatsSocket');

        factory(chai, sinon, ws, mats);
    } else {
        // Browser globals
        factory(chai, sinon, WebSocket, mats);
    }
}(typeof self !== 'undefined' ? self : this, function (chai, sinon, ws, mats) {
    const MatsSocket = mats.MatsSocket;

    let logging = false;

    // :: Make mock WebSocket
    const webSocket = {};
    // Make send function:
    webSocket.send = function (payload) {
        JSON.parse(payload).forEach(({t, cmid}, idx) => {
            if (t === 'HELLO') {
                setTimeout(() => {
                    webSocket.onmessage({target: webSocket, data: JSON.stringify([{t: "WELCOME"}])});
                }, idx);
            }
            if (t !== "ACK2" && cmid !== undefined) {
                setTimeout(() => {
                    webSocket.onmessage({target: webSocket, data: JSON.stringify([{t: "ACK", cmid: cmid}])});
                }, idx);
            }
        });
    };
    webSocket.close = function () {
    };

    // Make 'ononpen' property, which is set twice by MatsSocket: Once when it waits for it to open, and when this happens, it is "unset" (set to undefined)
    Object.defineProperty(webSocket, "onopen", {
        set(callback) {
            // When callback is set, immediately invoke it on next tick (fast opening times on this mock WebSocket..!)
            if (callback !== undefined) {
                setTimeout(() => callback({target: webSocket}), 0);
            }
        }
    });

    // MatsSocket config object specifying Mock WebSocket factory.
    let config = {webSocketFactory: () => webSocket};

    describe('MatsSocket unit tests', function () {
        describe('constructor', function () {
            it('Should fail on no arg invocation', function () {
                chai.assert.throws(() => new MatsSocket());
            });

            it('Should fail if appVersion and urls are missing', function () {
                chai.assert.throws(() => new MatsSocket("Test"));
            });

            it('Should fail if urls are missing', function () {
                chai.assert.throws(() => new MatsSocket("Test", "1.0"));
            });
            it('Should not invoke the WebSocket constructor', function () {
                const callback = sinon.spy(ws);
                new MatsSocket("Test", "1.0", ["ws://localhost:8080/"]);
                chai.assert(!callback.called);
                sinon.reset();
            });
        });

        describe('authorization callback', function () {
            it('Should invoke authorization callback before making calls', async function () {
                const matsSocket = new MatsSocket("Test", "1.0", ["ws://localhost:8080/"], config);
                matsSocket.logging = logging;

                let authCallbackCalled = false;
                matsSocket.setAuthorizationExpiredCallback(function (event) {
                    authCallbackCalled = true;
                    matsSocket.setCurrentAuthorization("Test", Date.now() + 20000, 0);
                });
                await matsSocket.send("Test.authCallback", "SEND_" + matsSocket.id(6), {});

                chai.assert(authCallbackCalled);

                matsSocket.close("Test done");
            });

            it('Should not invoke authorization callback if authorization present', async function () {
                const matsSocket = new MatsSocket("Test", "1.0", ["ws://localhost:8080/"], config);
                matsSocket.logging = logging;

                let authCallbackCalled = false;
                matsSocket.setCurrentAuthorization("Test", Date.now() + 20000, 0);
                matsSocket.setAuthorizationExpiredCallback(function (event) {
                    authCallbackCalled = true;
                });
                await matsSocket.send("Test.authCallback", "SEND_" + matsSocket.id(6), {});

                chai.assert(!authCallbackCalled);
                matsSocket.close("Test done");
            });

            it('Should invoke authorization callback when expired', async function () {
                const matsSocket = new MatsSocket("Test", "1.0", ["ws://localhost:8080/"], config);
                matsSocket.logging = logging;

                let authCallbackCalled = false;
                matsSocket.setCurrentAuthorization("Test", Date.now() - 20000, 0);
                matsSocket.setAuthorizationExpiredCallback(function (event) {
                    authCallbackCalled = true;
                    matsSocket.setCurrentAuthorization("Test", Date.now() + 20000, 0);
                });
                await matsSocket.send("Test.authCallback", "SEND_" + matsSocket.id(6), {});

                chai.assert(authCallbackCalled);
                matsSocket.close("Test done");
            });

            it('Should invoke authorization callback when room for latency expired', async function () {
                const matsSocket = new MatsSocket("Test", "1.0", ["ws://localhost:8080/"], config);
                matsSocket.logging = logging;

                let authCallbackCalled = false;
                matsSocket.setCurrentAuthorization("Test", Date.now() + 1000, 2000);
                matsSocket.setAuthorizationExpiredCallback(function (event) {
                    authCallbackCalled = true;
                    matsSocket.setCurrentAuthorization("Test", Date.now() + 20000, 0);
                });
                await matsSocket.send("Test.authCallback", "SEND_" + matsSocket.id(6), {});

                chai.assert(authCallbackCalled);
                matsSocket.close("Test done");
            });
        });

        describe('preconnectoperation callback', function () {
            it('Should invoke preconnectoperation callback if function', async function () {
                const url = "ws://localhost:8080/";
                const matsSocket = new MatsSocket("Test", "1.0", url, config);
                matsSocket.logging = logging;

                let preConnectRequestCalled = false;
                matsSocket.preconnectoperation = function (params) {
                    // Assert that the WebSocket URL that will be connected to is present.
                    chai.assert.strictEqual(params.webSocketUrl, url);

                    preConnectRequestCalled = true;

                    // Return value as contract stipulates.
                    let abortFunction = function () {
                        /* no-op abort, shall not be invoked in this test. */
                        chai.fail("Shall not be invoked");
                    };
                    let requestPromise = new Promise((resolve, reject) => {
                        resolve("rock on");
                    });
                    return [abortFunction, requestPromise];
                };

                let authCallbackCalled = false;

                matsSocket.setAuthorizationExpiredCallback(function (event) {
                    authCallbackCalled = true;
                    matsSocket.setCurrentAuthorization("Test", Date.now() + 20000, 0);
                });
                await matsSocket.send("Test.authCallback", "SEND_" + matsSocket.id(6), {});

                chai.assert(preConnectRequestCalled);
                chai.assert(authCallbackCalled);

                matsSocket.close("Test done");
            });
        });
    });

}));