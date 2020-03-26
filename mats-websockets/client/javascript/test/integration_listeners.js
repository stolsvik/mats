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

    describe('MatsSocket integration tests, listeners', function () {
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

        describe('InitiationProcessedEvent listeners', function () {
            // Set a valid authorization before each request
            beforeEach(() => setAuth());

            function runSendTest(includeInitiationMessage, done) {
                let traceId = "InitiationProcessedEvent_send_" + matsSocket.id(6);
                let msg = {
                    string: "The String",
                    number: Math.PI
                };

                let receivedRoundTripMillisFromReceived;

                function assertCommon(init) {
                    chai.assert.equal(init.type, mats.InitiationProcessedEventType.SEND);
                    chai.assert.equal(init.endpointId, "Test.single");
                    chai.assert.isTrue(init.sentTimestamp > 1585259649178);
                    chai.assert.isTrue(init.sessionEstablishedOffsetMillis < 0); // Since sent before WELCOME
                    chai.assert.equal(init.traceId, traceId);
                    chai.assert.isTrue(init.acknowledgeRoundTripMillis >= 1); // Should probably take more than 1 ms.
                    chai.assert.strictEqual(init.acknowledgeRoundTripMillis, receivedRoundTripMillisFromReceived);
                    // These are undefined for 'send':
                    chai.assert.isUndefined(init.replyMessageEventType);
                    chai.assert.isUndefined(init.replyToTerminatorId);
                    chai.assert.isUndefined(init.requestReplyRoundTripMillis);
                    chai.assert.isUndefined(init.replyMessageEvent);
                }

                matsSocket.addInitiationProcessedEventListener(function (processedEvent) {
                    // Assert common between matsSocket.initiations and listener event.
                    assertCommon(processedEvent);

                    // ?: Is initiationMessage included?
                    if (includeInitiationMessage) {
                        // -> Yes, so then it should be here
                        chai.assert.strictEqual(processedEvent.initiationMessage, msg);
                    } else {
                        // -> No, so then it should be undefined
                        chai.assert.isUndefined(processedEvent.initiationMessage);
                    }
                    assertCommon(processedEvent);
                    bothPresent();
                }, includeInitiationMessage, false);

                // First assert that there is no elements in 'initiations' before sending
                chai.assert.strictEqual(0, matsSocket.initiations.length);

                matsSocket.send("Test.single", traceId, msg).then(receivedEvent => {
                    // There should now be ONE initiation
                    // Note: Adding to matsSocket.initiations is sync, thus done before settling, but firing of InitiationProcessedEvent listeners is done using 'setTimeout(.., 0)'.
                    chai.assert.strictEqual(1, matsSocket.initiations.length);

                    // The received roundTripTime should be equal to the one in InitiationProcessedEvent
                    receivedRoundTripMillisFromReceived = receivedEvent.roundTripMillis;

                    let initiation = matsSocket.initiations[0];

                    // Assert common between matsSocket.initiations and listener event.
                    assertCommon(initiation);

                    // On matsSocket.initiations, the initiationMessage should always be present.
                    chai.assert.equal(initiation.initiationMessage, msg);

                    bothPresent();
                });

                // Since we cannot be certain which of these are invoked first, we'll just have to wait for both
                let count = 0;

                function bothPresent() {
                    count++;
                    if (count === 2) {
                        done();
                    }
                }
            }

            it('send: event should be present on matsSocket.initiations, and be issued to listener (with includeInitiationMessage=false).', function (done) {
                runSendTest(false, done);
            });
            it('send: event should be present on matsSocket.initiations, and be issued to listener (with includeInitiationMessage=true).', function (done) {
                runSendTest(true, done);
            });


            function runRequestTest(includeStash, done) {
                let traceId = "InitiationProcessedEvent_request_" + matsSocket.id(6);
                let msg = {
                    string: "The Strange",
                    number: Math.E
                };

                let initiationProcessedEventFromListener;
                let receivedRoundTripMillisFromReceived;
                let repliedMessageEvent;

                function assertCommon(init) {
                    chai.assert.equal(init.type, mats.InitiationProcessedEventType.REQUEST);
                    chai.assert.equal(init.endpointId, "Test.single");
                    chai.assert.isTrue(init.sentTimestamp > 1585259649178);
                    chai.assert.isTrue(init.sessionEstablishedOffsetMillis < 0); // Since sent before WELCOME
                    chai.assert.equal(init.traceId, traceId);
                    chai.assert.isTrue(init.acknowledgeRoundTripMillis >= 1); // Should probably take more than 1 ms.
                    chai.assert.strictEqual(init.acknowledgeRoundTripMillis, receivedRoundTripMillisFromReceived);
                    chai.assert.equal(init.replyMessageEventType, mats.MessageEventType.RESOLVE);
                    chai.assert.isTrue(init.requestReplyRoundTripMillis >= 1); // Should probably take more than 1 ms.
                }

                matsSocket.addInitiationProcessedEventListener(function (processedEvent) {
                    // Assert common between matsSocket.initiations and listener event.
                    assertCommon(processedEvent);

                    initiationProcessedEventFromListener = processedEvent;

                    // ?: Is initiationMessage included?
                    if (includeStash) {
                        // -> Yes, so then it should be here
                        chai.assert.strictEqual(processedEvent.initiationMessage, msg);
                    } else {
                        // -> No, so then it should be undefined
                        chai.assert.isUndefined(processedEvent.initiationMessage);
                    }
                    assertCommon(processedEvent);
                    bothPresent();
                }, includeStash, includeStash);

                // First assert that there is no elements in 'initiations' before sending
                chai.assert.strictEqual(0, matsSocket.initiations.length);

                matsSocket.request("Test.single", traceId, msg, function (receivedEvent) {
                    // The received roundTripTime should be equal to the one in InitiationProcessedEvent
                    receivedRoundTripMillisFromReceived = receivedEvent.roundTripMillis;
                }).then(messageEvent => {
                    // There should now be ONE initiation
                    // Note: Adding to matsSocket.initiations is sync, thus done before settling, but firing of InitiationProcessedEvent listeners is done using 'setTimeout(.., 0)'.
                    chai.assert.strictEqual(1, matsSocket.initiations.length);

                    // The MessageEvent should be same object as the one in InitiationProcessedEvent
                    repliedMessageEvent = messageEvent;

                    let initiation = matsSocket.initiations[0];

                    // Assert common between matsSocket.initiations and listener event.
                    assertCommon(initiation);

                    chai.assert.isUndefined(initiation.replyToTerminatorId);
                    chai.assert.strictEqual(initiation.replyMessageEvent, messageEvent);

                    // On matsSocket.initiations, the initiationMessage should always be present.
                    chai.assert.equal(initiation.initiationMessage, msg);

                    bothPresent();
                });

                // Since we cannot be certain which of these are invoked first, we'll just have to wait for both
                let count = 0;

                function bothPresent() {
                    count++;
                    if (count === 2) {
                        assertAlike();
                    }
                }

                function assertAlike() {
                    if (includeStash) {
                        chai.assert.strictEqual(initiationProcessedEventFromListener.replyMessageEvent, repliedMessageEvent);
                    } else {
                        chai.assert.isUndefined(initiationProcessedEventFromListener.replyMessageEvent);
                    }
                    done();
                }
            }

            it('request: event should be present on matsSocket.initiations, and be issued to listener (with includeInitiationMessage, includeReplyMessageEvent=false).', function (done) {
                runRequestTest(false, done);
            });
            it('request: event should be present on matsSocket.initiations, and be issued to listener (with includeInitiationMessage, includeReplyMessageEvent=true).', function (done) {
                runRequestTest(true, done);
            });


            function runRequestReplyToTest(includeStash, done) {
                let traceId = "InitiationProcessedEvent_request_" + matsSocket.id(6);
                let msg = {
                    string: "The Strange",
                    number: Math.E
                };

                let initiationProcessedEventFromListener;
                let receivedRoundTripMillisFromReceived;
                let repliedMessageEvent;

                function assertCommon(init) {
                    chai.assert.equal(init.type, mats.InitiationProcessedEventType.REQUEST_REPLY_TO);
                    chai.assert.equal(init.endpointId, "Test.single");
                    chai.assert.isTrue(init.sentTimestamp > 1585259649178);
                    chai.assert.isTrue(init.sessionEstablishedOffsetMillis < 0); // Since sent before WELCOME
                    chai.assert.equal(init.traceId, traceId);
                    chai.assert.isTrue(init.acknowledgeRoundTripMillis >= 1); // Should probably take more than 1 ms.
                    chai.assert.strictEqual(init.acknowledgeRoundTripMillis, receivedRoundTripMillisFromReceived);
                    chai.assert.equal(init.replyMessageEventType, mats.MessageEventType.RESOLVE);
                    chai.assert.isTrue(init.requestReplyRoundTripMillis >= 1); // Should probably take more than 1 ms.
                }

                matsSocket.addInitiationProcessedEventListener(function (processedEvent) {
                    // Assert common between matsSocket.initiations and listener event.
                    assertCommon(processedEvent);

                    initiationProcessedEventFromListener = processedEvent;

                    // ?: Is initiationMessage included?
                    if (includeStash) {
                        // -> Yes, so then it should be here
                        chai.assert.strictEqual(processedEvent.initiationMessage, msg);
                    } else {
                        // -> No, so then it should be undefined
                        chai.assert.isUndefined(processedEvent.initiationMessage);
                    }
                    assertCommon(processedEvent);
                    bothPresent();
                }, includeStash, includeStash);

                matsSocket.terminator("Test-terminator", function (messageEvent) {
                    // There should now be ONE initiation
                    // Note: Adding to matsSocket.initiations is sync, thus done before settling, but firing of InitiationProcessedEvent listeners is done using 'setTimeout(.., 0)'.
                    chai.assert.strictEqual(1, matsSocket.initiations.length);

                    // The MessageEvent should be same object as the one in InitiationProcessedEvent
                    repliedMessageEvent = messageEvent;

                    let initiation = matsSocket.initiations[0];

                    // Assert common between matsSocket.initiations and listener event.
                    assertCommon(initiation);

                    chai.assert.equal(initiation.replyToTerminatorId, "Test-terminator");
                    chai.assert.strictEqual(initiation.replyMessageEvent, messageEvent);

                    // On matsSocket.initiations, the initiationMessage should always be present.
                    chai.assert.equal(initiation.initiationMessage, msg);

                    bothPresent();
                });

                // First assert that there is no elements in 'initiations' before sending
                chai.assert.strictEqual(0, matsSocket.initiations.length);

                matsSocket.requestReplyTo("Test.single", traceId, msg, "Test-terminator", undefined)
                    .then(receivedEvent => {
                        // The received roundTripTime should be equal to the one in InitiationProcessedEvent
                        receivedRoundTripMillisFromReceived = receivedEvent.roundTripMillis;
                    });

                // Since we cannot be certain which of these are invoked first, we'll just have to wait for both
                let count = 0;

                function bothPresent() {
                    count++;
                    if (count === 2) {
                        assertAlike();
                    }
                }

                function assertAlike() {
                    if (includeStash) {
                        chai.assert.strictEqual(initiationProcessedEventFromListener.replyMessageEvent, repliedMessageEvent);
                    } else {
                        chai.assert.isUndefined(initiationProcessedEventFromListener.replyMessageEvent);
                    }
                    done();
                }
            }

            it('requestReplyTo: event should be present on matsSocket.initiations, and be issued to listener (with includeInitiationMessage, includeReplyMessageEvent=false).', function (done) {
                runRequestReplyToTest(false, done);
            });
            it('requestReplyTo: event should be present on matsSocket.initiations, and be issued to listener (with includeInitiationMessage, includeReplyMessageEvent=true).', function (done) {
                runRequestReplyToTest(true, done);
            });

        });
    });
}));