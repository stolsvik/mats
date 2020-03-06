// Register as an UMD module - source: https://github.com/umdjs/umd/blob/master/templates/commonjsStrict.js
(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        // AMD. Register as an anonymous module.
        define(['chai', 'sinon', 'ws', 'mats', 'env'], factory);
    } else if (typeof exports === 'object' && typeof exports.nodeName !== 'string') {
        // CommonJS
        const chai = require('chai');
        const sinon = require('sinon');
        const ws = require('ws');
        const mats = require('../lib/MatsSocket');

        factory(chai, sinon, ws, mats, process.env);
    } else {
        // Browser globals
        factory(chai, sinon, WebSocket, mats, {});
    }
}(typeof self !== 'undefined' ? self : this, function (chai, sinon, ws, mats, env) {
    const MatsSocket = mats.MatsSocket;

    describe('MatsSocket integration-tests of Server-side send/request', function () {
        let matsSocket;

        function setAuth(userId = "standard", duration = 20000, roomForLatencyMillis = 10000) {
            const now = Date.now();
            const expiry = now + duration;
            matsSocket.setCurrentAuthorization("DummyAuth:" + userId + ":" + expiry, expiry, roomForLatencyMillis);
        }

        const urls = env.MATS_SOCKET_URLS || "ws://localhost:8080/matssocket,ws://localhost:8081/matssocket";

        beforeEach(() => {
            matsSocket = new MatsSocket("TestApp", "1.2.3", urls.split(","));
            matsSocket.logging = false;
        });

        afterEach(() => {
            // :: Chill the close slightly, so as to get the final "ACKACK" envelope over to delete server's inbox.
            let toClose = matsSocket;
            setTimeout(function () {
                toClose.close("Test done");
            }, 25);
        });

        describe('MatsSocketServer.send()', function () {
            // Set a valid authorization before each request
            beforeEach(() => setAuth());

            it('Send a message to Server, which responds by sending a message to terminator at Client (us!), directly in the MatsStage', function (done) {
                let traceId = "MatsSocketServer.send_test_" + matsSocket.id(6);

                matsSocket.terminator("ClientSide.terminator", (msg) => {
                    chai.assert.strictEqual(msg.data.number, Math.E);
                    chai.assert.strictEqual(msg.traceId, traceId + ":SentFromMatsStage");
                    done()
                });
                matsSocket.send("Test.server.send.matsStage", traceId, {
                    number: Math.E
                })
            });

            it('Send a message to Server, which responds by sending a message to terminator at Client (us!), in a separate Thread', function (done) {
                let traceId = "MatsSocketServer.send_test_" + matsSocket.id(6);

                matsSocket.terminator("ClientSide.terminator", (msg) => {
                    chai.assert.strictEqual(msg.data.number, Math.E);
                    chai.assert.strictEqual(msg.traceId, traceId + ":SentFromThread");
                    done()
                });
                matsSocket.send("Test.server.send.thread", traceId, {
                    number: Math.E
                })
            });
        });

        describe('MatsSocketServer.request()', function () {
            // Set a valid authorization before each request
            beforeEach(() => setAuth());

            function doTest(startEndpoint, done) {
                let traceId = "MatsSocketServer.send_test_" + matsSocket.id(6);

                let initialMessage = "Message_" + matsSocket.id(20);

                // This endpoint will get a request from the Server, to which we respond - and the server will then send the reply back to the Terminator below.
                matsSocket.endpoint("ClientSide.endpoint", function (messageEvent) {
                    chai.assert.strictEqual(messageEvent.traceId, traceId);

                    return new Promise(function (resolve, reject) {
                        // Resolve it a tad later, to "emulate" some kind of processing
                        setTimeout(function () {
                            let data = messageEvent.data;
                            resolve({
                                string: data.string + ":From_IntegrationEndpointA",
                                number: data.number + Math.PI
                            });
                        }, 25);
                    });
                });

                // This terminator will get the final result from the Server
                matsSocket.terminator("ClientSide.terminator", (msg) => {
                    chai.assert.strictEqual(msg.data.number, Math.E + Math.PI);
                    chai.assert.strictEqual(msg.data.string, initialMessage + ":From_IntegrationEndpointA");
                    chai.assert.strictEqual(msg.traceId, traceId);
                    done()
                });

                // Here we send the message that starts the cascade
                matsSocket.send(startEndpoint, traceId, {
                    string: initialMessage,
                    number: Math.E
                })
            }

            it('Send a message to the Server, which responds by directly doing a Server-to-Client request (thus coming back here!), and when this returns to Server, sends it directly back', function (done) {
                doTest("Test.server.request.direct", done);
            });

            it('Send a message to the Server, which responds by, in a Mats terminator, doing a Server-to-Client request (thus coming back here!), and when this returns to Server, sends it back in a Mats terminator', function (done) {
                doTest("Test.server.request.direct", done);
            });
        });
    });
}));