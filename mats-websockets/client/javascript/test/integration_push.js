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

    describe('MatsSocket integration tests of Server-side send/request ("push")', function () {
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
            // :: Chill the close slightly, so as to get the final "ACK2" envelope over to delete server's inbox.
            // NOTE: This is done async, so for the fast tests, the closings will come in "behind".
            let toClose = matsSocket;
            setTimeout(function () {
                toClose.close("Test done");
            }, 500);
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

            function doTest(startEndpoint, resolveReject, done) {
                let traceId = "MatsSocketServer.send_test_" + matsSocket.id(6);

                let initialMessage = "Message_" + matsSocket.id(20);

                // This endpoint will get a request from the Server, to which we respond - and the server will then send the reply back to the Terminator below.
                matsSocket.endpoint("ClientSide.endpoint", function (messageEvent) {
                    chai.assert.strictEqual(messageEvent.traceId, traceId);

                    return new Promise(function (resolve, reject) {
                        // Resolve it a tad later, to "emulate" some kind of processing
                        setTimeout(function () {
                            let data = messageEvent.data;
                            let msg = {
                                string: data.string + ":From_IntegrationEndpointA",
                                number: data.number + Math.PI
                            };
                            // ?: Resolve or Reject based on param
                            if (resolveReject) {
                                resolve(msg);
                            } else {
                                reject(msg);
                            }

                        }, 25);
                    });
                });

                // This terminator will get the final result from the Server
                matsSocket.terminator("ClientSide.terminator", (msg) => {
                    chai.assert.strictEqual(msg.data.number, Math.E + Math.PI);
                    chai.assert.strictEqual(msg.data.string, initialMessage + ":From_IntegrationEndpointA:" + (resolveReject ? "RESOLVE" : "REJECT"));
                    chai.assert.strictEqual(msg.traceId, traceId);
                    done()
                });

                // Here we send the message that starts the cascade
                matsSocket.send(startEndpoint, traceId, {
                    string: initialMessage,
                    number: Math.E
                });
            }

            it('Send a message to the Server, which responds by directly doing a Server-to-Client request (thus coming back here!), resolves, and when this returns to Server, sends it back to a Client Terminator', function (done) {
                doTest("Test.server.request.direct", true, done);
            });

            it('Send a message to the Server, which responds by directly doing a Server-to-Client request (thus coming back here!), rejects, and when this returns to Server, sends it back to a Client Terminator', function (done) {
                doTest("Test.server.request.direct", false, done);
            });

            it('Send a message to the Server, which responds by - in a Mats terminator - doing a Server-to-Client request (thus coming back here!), resolve, and when this returns to Server, sends it back to a Client Terminator', function (done) {
                doTest("Test.server.request.viaMats", true, done);
            });

            it('Send a message to the Server, which responds by - in a Mats terminator - doing a Server-to-Client request (thus coming back here!), rejects, and when this returns to Server, sends it back to a Client Terminator', function (done) {
                doTest("Test.server.request.viaMats", false, done);
            });
        });

        describe('Server initiated sends and requests - with DebugOptions', function () {
            function extracted(msg, debugOptions) {
                // This is a server-initiated message, so these should be undefined in MessageEvent
                chai.assert.isUndefined(msg.clientRequestTimestamp);
                chai.assert.isUndefined(msg.roundTripMillis);

                // :: Assert debug
                // It should be present in MessageEvent, since it should be in the message from the server
                chai.assert.isDefined(msg.debug);
                // These should be set
                if ((debugOptions & mats.DebugOption.TIMESTAMPS) > 0) {
                    chai.assert.isNumber(msg.debug.serverMessageCreated);
                    chai.assert.isNumber(msg.debug.messageSentToClient);
                } else {
                    chai.assert.isUndefined(msg.debug.serverMessageCreated);
                    chai.assert.isUndefined(msg.debug.messageSentToClient);
                }
                if ((debugOptions & mats.DebugOption.NODES) > 0) {
                    chai.assert.isDefined(msg.debug.serverMessageCreatedNodename);
                    chai.assert.isDefined(msg.debug.messageSentToClientNodename);
                } else {
                    chai.assert.isUndefined(msg.debug.serverMessageCreatedNodename);
                    chai.assert.isUndefined(msg.debug.messageSentToClientNodename);
                }
                // While all other should not be set
                chai.assert.isUndefined(msg.debug.clientMessageSent);
                chai.assert.isUndefined(msg.debug.clientMessageReceived);
                chai.assert.isUndefined(msg.debug.clientMessageReceivedNodename);
                chai.assert.isUndefined(msg.debug.matsMessageSent);
                chai.assert.isUndefined(msg.debug.matsMessageReplyReceived);
                chai.assert.isUndefined(msg.debug.matsMessageReplyReceivedNodename);

                // This is not a request from Client, so there is no requested debug options.
                chai.assert.isUndefined(msg.debug.requestedDebugOptions);
                // This is the resolved from what we asked server to use above, and what we are allowed to.
                chai.assert.equal(msg.debug.resolvedDebugOptions, matsSocket.debug);
            }

            function testServerInitiatedSend_DebugOptions(debugOptions, done) {
                // Set special userId that gives us all DebugOptions
                setAuth('enableAllDebugOptions');

                let traceId = "MatsSocketServer.DebugOptions_server.send_" + matsSocket.id(6);

                // These will become the server's initiation requested DebugOptions upon the subsequent 'send'
                matsSocket.debug = debugOptions;

                matsSocket.terminator("ClientSide.terminator", (msg) => {
                    chai.assert.strictEqual(msg.data.number, Math.E);
                    chai.assert.strictEqual(msg.traceId, traceId + ":SentFromThread");

                    extracted(msg, debugOptions);
                    done()
                });

                matsSocket.send("Test.server.send.thread", traceId, {
                    number: Math.E
                })
            }

            it('Server initiated send should have debug object if user asks for it - with the all server-sent stuff filled.', function (done) {
                testServerInitiatedSend_DebugOptions(mats.DebugOption.NODES | mats.DebugOption.TIMESTAMPS, done);
            });
            it('Server initiated send should have debug object if user asks for just nodes - with just the nodes filled.', function (done) {
                testServerInitiatedSend_DebugOptions(mats.DebugOption.NODES, done);
            });
            it('Server initiated send should have debug object if user asks for just timestamps - with just the timestamps filled.', function (done) {
                testServerInitiatedSend_DebugOptions(mats.DebugOption.TIMESTAMPS, done);
            });

            function testServerInitiatedRequest_DebugOptions(debugOptions, done) {
                // Set special userId that gives us all DebugOptions
                setAuth('enableAllDebugOptions');

                let traceId = "MatsSocketServer.DebugOptions_server.send_" + matsSocket.id(6);

                // These will become the server's initiation requested DebugOptions upon the subsequent 'send'
                matsSocket.debug = debugOptions;
                let initialMessage = "Message_" + matsSocket.id(20);

                matsSocket.endpoint("ClientSide.endpoint", (msg) => {
                    chai.assert.strictEqual(msg.data.number, Math.E);
                    chai.assert.strictEqual(msg.traceId, traceId);
                    extracted(msg, debugOptions);

                    return Promise.resolve({
                        string: msg.data.string + ":From_DebugOptions_test",
                        number: msg.data.number + Math.PI
                    })
                });

                // This terminator will get the final result from the Server
                matsSocket.terminator("ClientSide.terminator", (msg) => {
                    chai.assert.strictEqual(msg.data.number, Math.E + Math.PI);
                    chai.assert.strictEqual(msg.data.string, initialMessage + ":From_DebugOptions_test:RESOLVE");
                    chai.assert.strictEqual(msg.traceId, traceId);
                    done()
                });

                matsSocket.send("Test.server.request.direct", traceId, {
                    string: initialMessage,
                    number: Math.E
                })
            }
            it('Server initiated request should have debug object if user asks for it - with the all server-sent stuff filled.', function (done) {
                testServerInitiatedRequest_DebugOptions(mats.DebugOption.NODES | mats.DebugOption.TIMESTAMPS, done);
            });
            it('Server initiated request should have debug object if user asks for just nodes - with just the nodes filled.', function (done) {
                testServerInitiatedRequest_DebugOptions(mats.DebugOption.NODES, done);
            });
            it('Server initiated request should have debug object if user asks for just timestamps - with just the timestamps filled.', function (done) {
                testServerInitiatedRequest_DebugOptions(mats.DebugOption.TIMESTAMPS, done);
            });

            function serverInitiatedSend_NoDebugOptions(done, debugOptions) {
                // Set special userId that gives us all DebugOptions
                setAuth('enableAllDebugOptions');

                let traceId = "MatsSocketServer.DebugOptions_server.send_" + matsSocket.id(6);

                matsSocket.terminator("ClientSide.terminator", (msg) => {
                    chai.assert.strictEqual(msg.data.number, Math.E);
                    chai.assert.strictEqual(msg.traceId, traceId + ":SentFromThread");

                    // This is a server-initiated message, so these should be undefined in MessageEvent
                    chai.assert.isUndefined(msg.clientRequestTimestamp);
                    chai.assert.isUndefined(msg.roundTripMillis);

                    // :: Assert debug
                    // Since this is server-initiated, '0' will not give debug object - i.e. the server does not have difference between '0' and 'undefined'.
                    chai.assert.isUndefined(msg.debug);
                    done()
                });

                // :: These will become the server's initiation requested DebugOptions upon the subsequent 'send'
                // First set it to "all the things!"
                matsSocket.debug = 255;

                matsSocket.send("Test.single", "DebugOptions_set_to_all", {
                    number: Math.E
                });

                // Then set it to none (0 or undefined), which should result in NO debug object from server-initiated messages
                matsSocket.debug = debugOptions;

                matsSocket.send("Test.server.send.thread", traceId, {
                    number: Math.E
                })

            }
            it('Server initiated send should NOT have debug object if user ask for \'0\'.', function (done) {
                serverInitiatedSend_NoDebugOptions(done, 0);
            });
            it('Server initiated send should NOT have debug object if user ask for \'undefined\'.', function (done) {
                serverInitiatedSend_NoDebugOptions(done, undefined);
            });

        })
    });
}));