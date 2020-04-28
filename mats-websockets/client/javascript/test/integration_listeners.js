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
            // :: Chill the close slightly, so as to get the final "ACK2" envelope over to delete server's inbox.
            // NOTE: This is done async, so for the fast tests, the closings will come in "behind".
            let toClose = matsSocket;
            setTimeout(function () {
                toClose.close("Test done");
            }, 250);
        });

        // TODO: Check ConnectionEventListeners, including matsSocket.state

        // ===================================================================================
        // == NOTE: SessionClosedEventListener is tested in "connect, reconnect and close". ==
        // ===================================================================================

        describe('PingPing listeners', function () {
            // Set a valid authorization before each request
            beforeEach(() => setAuth());

            it('PingPong listener.', function (done) {
                matsSocket.initialPingDelay = 50;
                matsSocket.addPingPongListener(function(pingPong) {
                    chai.assert.equal(pingPong.pingId, 0);
                    chai.assert.isTrue(pingPong.sentTimestamp > 1585856935097);
                    chai.assert.isTrue(pingPong.roundTripMillis >= 0);  // Might take 0.4ms, and Firefox will round that to 0.
                    done();
                });
                matsSocket.send("Test.ignoreInIncomingHandler", "PingPong_listener_"+matsSocket.id(6), {});
            });

        });


        describe("ErrorEvent listener", function () {
            it("Throwing an error in InitiationProcessedEvent listener should invoke ErrorEvent listeners", function (done) {
                setAuth();

                let err = {
                    error: "test error!",
                    test: "test again!"
                };
                function assertErrorEvent(errorEvent) {
                    chai.assert.equal(errorEvent.type, "notify InitiationProcessedEvent listeners");
                    chai.assert.isTrue(errorEvent.message.includes("InitiationProcessedEvent"));
                    chai.assert.strictEqual(errorEvent.reference, err);
                    chai.assert.equal(errorEvent.referenceAsString(), "{\"error\":\"test error!\",\"test\":\"test again!\"}");
                }

                let numberOfListenersInvoked = 0;
                matsSocket.addErrorEventListener(function (errorEvent) {
                    assertErrorEvent(errorEvent);
                    numberOfListenersInvoked++;
                });
                matsSocket.addErrorEventListener(function (errorEvent) {
                    assertErrorEvent(errorEvent);
                    numberOfListenersInvoked++;
                });

                matsSocket.addInitiationProcessedEventListener(function (initiationProcessedEvent) {
                    // We throw here, which should invoke our two ErrorEvent listeners.
                    throw err;
                });

                let received = false;
                let promise = matsSocket.request("Test.resolveInIncomingHandler", "ErrorEvent-listener_" + matsSocket.id(6), {},
                    function () {
                        received = true;
                    });
                promise.then(function () {
                    chai.assert(received, "The received-callback should have been invoked.");
                    chai.assert.equal(numberOfListenersInvoked, 2);
                    done();
                });
            });

            it("Missing AuthenticationCallback should invoke ErrorEvent listeners", function (done) {
                function assertErrorEvent(errorEvent) {
                    chai.assert.strictEqual(errorEvent.type, "missingauthcallback");
                    chai.assert.isTrue(errorEvent.message.includes("missing 'authorizationExpiredCallback'"));
                    chai.assert.isUndefined(errorEvent.reference);
                }

                let numberOfListenersInvoked = 0;
                matsSocket.addErrorEventListener(function (errorEvent) {
                    assertErrorEvent(errorEvent);
                    numberOfListenersInvoked++;
                    doneWhenTwo();
                });
                matsSocket.addErrorEventListener(function (errorEvent) {
                    assertErrorEvent(errorEvent);
                    numberOfListenersInvoked++;
                    doneWhenTwo();
                });

                function doneWhenTwo() {
                    // Nothing should be invoked of the receivedCallback or Request-Promise
                    chai.assert.isFalse(anythingInvoked);
                    // ?: Are two ErrorEvent listeners invoked?
                    if (numberOfListenersInvoked === 2) {
                        // -> Yes, and then the test is done.
                        done();
                    }
                }

                // Nothing of this should be invoked at the point where the test checks.
                let anythingInvoked = false;
                matsSocket.request("Test.resolveInIncomingHandler", "ErrorEvent-listener_" + matsSocket.id(6), {},
                    function () {
                        anythingInvoked = true;
                    })
                    .then(_ => {
                        anythingInvoked = true;
                    })
                    .catch(_ => {
                        anythingInvoked = true;
                    });
            });
        });

        describe('InitiationProcessedEvent listeners', function () {
            // Set a valid authorization before each request
            beforeEach(() => setAuth());

            function runSendTest(includeInitiationMessage, done) {
                let traceId = "InitiationProcessedEvent_send_" + includeInitiationMessage + "_" + matsSocket.id(6);
                let msg = {
                    string: "The String",
                    number: Math.PI
                };

                function assertCommon(init) {
                    chai.assert.equal(init.type, mats.InitiationProcessedEventType.SEND);
                    chai.assert.equal(init.endpointId, "Test.single");
                    chai.assert.isTrue(init.sentTimestamp > 1585259649178);
                    chai.assert.isTrue(init.sessionEstablishedOffsetMillis < 0, "sessionEstablishedOffsetMillis should be negative since sent before WELCOME, was [" + init.sessionEstablishedOffsetMillis + "]");
                    chai.assert.equal(init.traceId, traceId);
                    chai.assert.isTrue(init.acknowledgeRoundTripMillis >= 1); // Should probably take more than 1 ms.
                    // These are undefined for 'send':
                    chai.assert.isUndefined(init.replyMessageEventType);
                    chai.assert.isUndefined(init.replyToTerminatorId);
                    chai.assert.isUndefined(init.requestReplyRoundTripMillis);
                    chai.assert.isUndefined(init.replyMessageEvent);
                }

                /*
                 *  This is a send. Order should be:
                 *  - ReceivedEvent, by settling the returned Received-Promise from invocation of 'send'.
                 *  - InitiationProcessedEvent on matsSocket.initiations
                 *  - InitiationProcessedEvent on listeners
                 */

                let receivedRoundTripMillisFromReceived;
                let initiationProcessedEventFromListener;

                matsSocket.addInitiationProcessedEventListener(function (processedEvent) {
                    // Assert common between matsSocket.initiations and listener event.
                    assertCommon(processedEvent);

                    // ReceivedEvent shall have been processed, as that is the first in order
                    chai.assert.isDefined(receivedRoundTripMillisFromReceived, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}}: experienced InitiationProcessedEvent (listener) BEFORE ReceivedEvent");
                    // The matsSocket.initiations should be there, as listeners are after
                    chai.assert.equal(matsSocket.initiations.length, 1, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}}: experienced InitiationProcessedEvent (listener) BEFORE InitiationProcessedEvent (matsSocket.initiations)");

                    // Assert common between matsSocket.initiations and listener event.
                    assertCommon(matsSocket.initiations[0]);

                    initiationProcessedEventFromListener = processedEvent;

                    // ?: Is initiationMessage included?
                    if (includeInitiationMessage) {
                        // -> Yes, so then it should be here
                        chai.assert.strictEqual(processedEvent.initiationMessage, msg);
                    } else {
                        // -> No, so then it should be undefined
                        chai.assert.isUndefined(processedEvent.initiationMessage);
                    }

                    let initiation = matsSocket.initiations[0];
                    // On matsSocket.initiations, the initiationMessage should always be present.
                    chai.assert.equal(initiation.initiationMessage, msg);
                    // The ackRoundTrip should again be equal to received.
                    chai.assert.strictEqual(initiation.acknowledgeRoundTripMillis, receivedRoundTripMillisFromReceived);

                    done();

                }, includeInitiationMessage, false);

                // First assert that there is no elements in 'initiations' before sending
                chai.assert.strictEqual(0, matsSocket.initiations.length);

                matsSocket.send("Test.single", traceId, msg).then(receivedEvent => {
                    // ORDERING: ReceivedEvent shall be first in line, before InitiationProcessedEvent and MessageEvent.
                    chai.assert.equal(matsSocket.initiations.length, 0, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}}: experienced ReceivedEvent AFTER InitiationProcessedEvent (matsSocket.initiations)");
                    chai.assert.isUndefined(initiationProcessedEventFromListener, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}}: experienced ReceivedEvent AFTER InitiationProcessedEvent (listeners)");

                    // The received roundTripTime should be equal to the one in InitiationProcessedEvent
                    receivedRoundTripMillisFromReceived = receivedEvent.roundTripMillis;
                });
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

                let receivedRoundTripMillisFromReceived;

                function assertCommon(init) {
                    chai.assert.equal(init.type, mats.InitiationProcessedEventType.REQUEST);
                    chai.assert.equal(init.endpointId, "Test.single");
                    chai.assert.isTrue(init.sentTimestamp > 1585259649178);
                    chai.assert.isTrue(init.sessionEstablishedOffsetMillis < 0, "sessionEstablishedOffsetMillis should be negative since sent before WELCOME, was [" + init.sessionEstablishedOffsetMillis + "]");
                    chai.assert.equal(init.traceId, traceId);
                    chai.assert.isTrue(init.acknowledgeRoundTripMillis >= 1); // Should probably take more than 1 ms.
                    chai.assert.strictEqual(init.acknowledgeRoundTripMillis, receivedRoundTripMillisFromReceived);
                    chai.assert.equal(init.replyMessageEventType, mats.MessageEventType.RESOLVE);
                    chai.assert.isTrue(init.requestReplyRoundTripMillis >= 1); // Should probably take more than 1 ms.
                }

                /*
                 *  This is a 'request'. Order should be:
                 *  - ReceivedEvent - invoked on receivedCallback supplied in 'request'-invocation.
                 *  - InitiationProcessedEvent on matsSocket.initiations
                 *  - InitiationProcessedEvent on listeners
                 *  - MessageEvent, by settling the returned Reply-Promise from invocation of 'request'.
                 */

                let initiationProcessedEventFromListener;
                let repliedMessageEvent;

                matsSocket.addInitiationProcessedEventListener(function (processedEvent) {
                    // Assert common between matsSocket.initiations and listener event.
                    assertCommon(processedEvent);

                    // ReceivedEvent shall have been processed, as that is the first in order
                    chai.assert.isDefined(receivedRoundTripMillisFromReceived, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced InitiationProcessedEvent (listener) BEFORE ReceivedEvent");
                    // The matsSocket.initiations should be there, as listeners are after
                    chai.assert.equal(matsSocket.initiations.length, 1, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced InitiationProcessedEvent (listener) BEFORE InitiationProcessedEvent (matsSocket.initiations)");
                    // The MessageEvent shall not have come yet, we are earlier.
                    chai.assert.isUndefined(repliedMessageEvent, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced InitiationProcessedEvent (listener) AFTER MessageEvent");

                    assertCommon(matsSocket.initiations[0]);

                    initiationProcessedEventFromListener = processedEvent;

                    // ?: Is initiationMessage included?
                    if (includeStash) {
                        // -> Yes, so then it should be here
                        chai.assert.strictEqual(processedEvent.initiationMessage, msg);
                    } else {
                        // -> No, so then it should be undefined
                        chai.assert.isUndefined(processedEvent.initiationMessage);
                    }

                }, includeStash, includeStash);

                // First assert that there is no elements in 'initiations' before sending
                chai.assert.strictEqual(0, matsSocket.initiations.length);


                // Perform the request, with a receivedCallback, which produces a Promise<MessageEvent>.
                matsSocket.request("Test.single", traceId, msg, function (receivedEvent) {
                    // ORDERING: ReceivedEvent shall be first in line, before InitiationProcessedEvent and MessageEvent.
                    chai.assert.isUndefined(repliedMessageEvent, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced ReceivedEvent AFTER MessageEvent");
                    chai.assert.equal(matsSocket.initiations.length, 0, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced ReceivedEvent AFTER InitiationProcessedEvent (matsSocket.initiations)");
                    chai.assert.isUndefined(initiationProcessedEventFromListener, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced ReceivedEvent AFTER InitiationProcessedEvent (listeners)");

                    // The received roundTripTime should be equal to the one in InitiationProcessedEvent
                    receivedRoundTripMillisFromReceived = receivedEvent.roundTripMillis;
                }).then(messageEvent => {
                    // ReceivedEvent shall have been processed, as that is the first in order
                    chai.assert.isDefined(receivedRoundTripMillisFromReceived, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced MessageEvent BEFORE ReceivedEvent");
                    // There should now be ONE matsSocket.initiation already in place, as MessageEvent shall be the latest
                    chai.assert.strictEqual(1, matsSocket.initiations.length, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced MessageEvent BEFORE InitiationProcessedEvent (matsSocket.initiations)");
                    // The InitiationProcessedEvent listener should have been invoked, as MessageEvent shall be the latest
                    chai.assert.isDefined(initiationProcessedEventFromListener, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced MessageEvent BEFORE InitiationProcessedEvent (listener)");

                    // We've received the MessageEvent
                    repliedMessageEvent = messageEvent;

                    let initiation = matsSocket.initiations[0];

                    // Assert common between matsSocket.initiations and listener event.
                    assertCommon(initiation);

                    // This is not a requestReplyTo
                    chai.assert.isUndefined(initiation.replyToTerminatorId);
                    // On matsSocket.initiations, the initiationMessage should always be present.
                    chai.assert.equal(initiation.initiationMessage, msg);
                    // On matsSocket.initiations, the replyMessageEvent should always be present.
                    chai.assert.strictEqual(initiation.replyMessageEvent, messageEvent);

                    // ?: If we asked to include it for listener, then the replyMessageEvent should be the same object as we got here
                    if (includeStash) {
                        chai.assert.strictEqual(initiationProcessedEventFromListener.replyMessageEvent, messageEvent);
                    } else {
                        chai.assert.isUndefined(initiationProcessedEventFromListener.replyMessageEvent);
                    }

                    done();
                });
            }

            it('request: Event should be present on matsSocket.initiations, and be issued to listener (with includeInitiationMessage, includeReplyMessageEvent=false).', function (done) {
                runRequestTest(false, done);
            });
            it('request: Event should be present on matsSocket.initiations, and be issued to listener (with includeInitiationMessage, includeReplyMessageEvent=true).', function (done) {
                runRequestTest(true, done);
            });


            function runRequestReplyToTest(includeStash, done) {
                let traceId = "InitiationProcessedEvent_requestReplyTo_" + matsSocket.id(6);
                let msg = {
                    string: "The Curious",
                    number: Math.PI
                };

                let receivedRoundTripMillisFromReceived;

                function assertCommon(init) {
                    chai.assert.equal(init.type, mats.InitiationProcessedEventType.REQUEST_REPLY_TO);
                    chai.assert.equal(init.endpointId, "Test.single");
                    chai.assert.isTrue(init.sentTimestamp > 1585259649178);
                    chai.assert.isTrue(init.sessionEstablishedOffsetMillis < 0, "sessionEstablishedOffsetMillis should be negative since sent before WELCOME, was [" + init.sessionEstablishedOffsetMillis + "]");
                    chai.assert.equal(init.traceId, traceId);
                    chai.assert.isTrue(init.acknowledgeRoundTripMillis >= 1); // Should probably take more than 1 ms.
                    chai.assert.strictEqual(init.acknowledgeRoundTripMillis, receivedRoundTripMillisFromReceived);
                    chai.assert.equal(init.replyMessageEventType, mats.MessageEventType.RESOLVE);
                    chai.assert.isTrue(init.requestReplyRoundTripMillis >= 1); // Should probably take more than 1 ms.
                }

                /*
                 *  This is a 'requestReplyTo'. Order should be:
                 *  - ReceivedEvent - on returned Received-Promise from invocation of 'requestReplyTo'
                 *  - InitiationProcessedEvent on matsSocket.initiations
                 *  - InitiationProcessedEvent on listeners
                 *  - MessageEvent, sent to Terminator by invocation of its 'messageCallback'
                 */
                let initiationProcessedEventFromListener;
                let repliedMessageEvent;

                matsSocket.addInitiationProcessedEventListener(function (processedEvent) {
                    // Assert common between matsSocket.initiations and listener event.
                    assertCommon(processedEvent);

                    // ReceivedEvent shall have been processed, as that is the first in order
                    chai.assert.isDefined(receivedRoundTripMillisFromReceived, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced InitiationProcessedEvent (listener) BEFORE ReceivedEvent");
                    // The matsSocket.initiations should be there, as listeners are after
                    chai.assert.equal(matsSocket.initiations.length, 1, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced InitiationProcessedEvent (listener) BEFORE InitiationProcessedEvent (matsSocket.initiations)");
                    // The MessageEvent shall not have come yet, we are earlier.
                    chai.assert.isUndefined(repliedMessageEvent, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced InitiationProcessedEvent (listener) AFTER MessageEvent");

                    assertCommon(matsSocket.initiations[0]);

                    initiationProcessedEventFromListener = processedEvent;

                    // ?: Is initiationMessage included?
                    if (includeStash) {
                        // -> Yes, so then it should be here
                        chai.assert.strictEqual(processedEvent.initiationMessage, msg);
                    } else {
                        // -> No, so then it should be undefined
                        chai.assert.isUndefined(processedEvent.initiationMessage);
                    }
                }, includeStash, includeStash);

                matsSocket.terminator("Test-terminator", function (messageEvent) {
                    // ReceivedEvent shall have been processed, as that is the first in order
                    chai.assert.isDefined(receivedRoundTripMillisFromReceived, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced MessageEvent BEFORE ReceivedEvent");
                    // There should now be ONE matsSocket.initiation already in place, as MessageEvent shall be the latest
                    chai.assert.strictEqual(1, matsSocket.initiations.length, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced MessageEvent BEFORE InitiationProcessedEvent (matsSocket.initiations)");
                    // The InitiationProcessedEvent listener should have been invoked, as MessageEvent shall be the latest
                    chai.assert.isDefined(initiationProcessedEventFromListener, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced MessageEvent BEFORE InitiationProcessedEvent (listener)");

                    // We've received the MessageEvent
                    repliedMessageEvent = messageEvent;

                    let initiation = matsSocket.initiations[0];

                    // Assert common between matsSocket.initiations and listener event.
                    assertCommon(initiation);

                    // This IS a requestReplyTo
                    chai.assert.equal(initiation.replyToTerminatorId, "Test-terminator");
                    // On matsSocket.initiations, the initiationMessage should always be present.
                    chai.assert.equal(initiation.initiationMessage, msg);
                    // On matsSocket.initiations, the replyMessageEvent should always be present.
                    chai.assert.strictEqual(initiation.replyMessageEvent, messageEvent);

                    // ?: If we asked to include it for listener, then the replyMessageEvent should be the same object as we got here
                    if (includeStash) {
                        chai.assert.strictEqual(initiationProcessedEventFromListener.replyMessageEvent, messageEvent);
                    } else {
                        chai.assert.isUndefined(initiationProcessedEventFromListener.replyMessageEvent);
                    }

                    done();
                });

                // First assert that there is no elements in 'initiations' before sending
                chai.assert.strictEqual(0, matsSocket.initiations.length);

                // Perform the requestReplyTo, which produces a Promise<ReceivedEvent>
                matsSocket.requestReplyTo("Test.single", traceId, msg, "Test-terminator", undefined)
                    .then(receivedEvent => {
                        // ORDERING: ReceivedEvent shall be first in line, before InitiationProcessedEvent and MessageEvent.
                        chai.assert.isUndefined(repliedMessageEvent, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced ReceivedEvent AFTER MessageEvent");
                        chai.assert.equal(matsSocket.initiations.length, 0, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced ReceivedEvent AFTER InitiationProcessedEvent (matsSocket.initiations)");
                        chai.assert.isUndefined(initiationProcessedEventFromListener, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced ReceivedEvent AFTER InitiationProcessedEvent (listeners)");

                        // The received roundTripTime should be equal to the one in InitiationProcessedEvent
                        receivedRoundTripMillisFromReceived = receivedEvent.roundTripMillis;
                    });
            }

            it('requestReplyTo: Event should be present on matsSocket.initiations, and be issued to listener (with includeInitiationMessage, includeReplyMessageEvent=false).', function (done) {
                runRequestReplyToTest(false, done);
            });
            it('requestReplyTo: Event should be present on matsSocket.initiations, and be issued to listener (with includeInitiationMessage, includeReplyMessageEvent=true).', function (done) {
                runRequestReplyToTest(true, done);
            });

            function runRequestReplyToWithTimeoutTest(includeStash, replyTimeout, done) {
                let traceId = "InitiationProcessedEvent_requestReplyToWithTimeout_" + matsSocket.id(6);
                let msg = {
                    string: "The Straight-Out Frightening",
                    number: Math.SQRT2,
                    sleepIncoming: 150, // Sleeptime before acknowledging - longer than the timeout.
                    sleepTime: 10 // Sleeptime before replying
                };

                let receivedRoundTripMillisFromReceived;

                function assertCommon(init) {
                    chai.assert.equal(init.type, mats.InitiationProcessedEventType.REQUEST_REPLY_TO);
                    chai.assert.equal(init.endpointId, "Test.slow");
                    chai.assert.isTrue(init.sentTimestamp > 1585259649178);
                    // Note: Will be zero if session is not established yet.
                    chai.assert.isTrue(init.sessionEstablishedOffsetMillis <= 0, "sessionEstablishedOffsetMillis should be zero or negative, since sent before WELCOME, was [" + init.sessionEstablishedOffsetMillis + "]");
                    chai.assert.equal(init.traceId, traceId);
                    // Evidently, we sometimes get 0 ms - probably only when doing timeout with 0 ms in this test.
                    chai.assert.isTrue(init.acknowledgeRoundTripMillis >= 0);
                    chai.assert.strictEqual(init.acknowledgeRoundTripMillis, receivedRoundTripMillisFromReceived);
                    chai.assert.equal(init.replyMessageEventType, mats.MessageEventType.TIMEOUT);
                    // Might sometimes get 0 ms - probably only when doing timeout with 0 ms in this test.
                    chai.assert.isTrue(init.requestReplyRoundTripMillis >= 0); // Evidently, we sometimes get 0 ms
                }

                /*
                 *  This is a 'requestReplyTo'. Order should be:
                 *  - ReceivedEvent - on returned Received-Promise from invocation of 'requestReplyTo'
                 *  - InitiationProcessedEvent on matsSocket.initiations
                 *  - InitiationProcessedEvent on listeners
                 *  - MessageEvent, sent to Terminator by invocation of its 'messageCallback'
                 */
                let initiationProcessedEventFromListener;
                let repliedMessageEvent;

                matsSocket.addInitiationProcessedEventListener(function (processedEvent) {
                    // Assert common between matsSocket.initiations and listener event.
                    assertCommon(processedEvent);

                    // ReceivedEvent shall have been processed, as that is the first in order
                    chai.assert.isDefined(receivedRoundTripMillisFromReceived, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced InitiationProcessedEvent (listener) BEFORE ReceivedEvent");
                    // The matsSocket.initiations should be there, as listeners are after
                    chai.assert.equal(matsSocket.initiations.length, 1, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced InitiationProcessedEvent (listener) BEFORE InitiationProcessedEvent (matsSocket.initiations)");
                    // The MessageEvent shall not have come yet, we are earlier.
                    chai.assert.isUndefined(repliedMessageEvent, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced InitiationProcessedEvent (listener) AFTER MessageEvent");

                    assertCommon(matsSocket.initiations[0]);

                    initiationProcessedEventFromListener = processedEvent;

                    // ?: Is initiationMessage included?
                    if (includeStash) {
                        // -> Yes, so then it should be here
                        chai.assert.strictEqual(processedEvent.initiationMessage, msg);
                    } else {
                        // -> No, so then it should be undefined
                        chai.assert.isUndefined(processedEvent.initiationMessage);
                    }
                }, includeStash, includeStash);

                let messageCallbackInvoked = false;
                matsSocket.terminator("Test-terminator", function (messageEvent) {
                    console.log("TERMINATOR WAS RESOLVED (messageCallback!) - this should NOT happen!", messageEvent);
                    messageCallbackInvoked = true;
                }, function (messageEvent) {
                    // The messageCallback shall not have been invoked
                    chai.assert.isFalse(messageCallbackInvoked, "messageCallback should NOT have been invoked");

                    // ReceivedEvent shall have been processed, as that is the first in order
                    chai.assert.isDefined(receivedRoundTripMillisFromReceived, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced MessageEvent BEFORE ReceivedEvent");
                    // There should now be ONE matsSocket.initiation already in place, as MessageEvent shall be the latest
                    chai.assert.strictEqual(1, matsSocket.initiations.length, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced MessageEvent BEFORE InitiationProcessedEvent (matsSocket.initiations)");
                    // The InitiationProcessedEvent listener should have been invoked, as MessageEvent shall be the latest
                    chai.assert.isDefined(initiationProcessedEventFromListener, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced MessageEvent BEFORE InitiationProcessedEvent (listener)");

                    // We've received the MessageEvent
                    repliedMessageEvent = messageEvent;

                    let initiation = matsSocket.initiations[0];

                    // Assert common between matsSocket.initiations and listener event.
                    assertCommon(initiation);

                    // This IS a requestReplyTo
                    chai.assert.equal(initiation.replyToTerminatorId, "Test-terminator");
                    // On matsSocket.initiations, the initiationMessage should always be present.
                    chai.assert.equal(initiation.initiationMessage, msg);
                    // On matsSocket.initiations, the replyMessageEvent should always be present.
                    chai.assert.strictEqual(initiation.replyMessageEvent, messageEvent);

                    // ?: If we asked to include it for listener, then the replyMessageEvent should be the same object as we got here
                    if (includeStash) {
                        chai.assert.strictEqual(initiationProcessedEventFromListener.replyMessageEvent, messageEvent);
                    } else {
                        chai.assert.isUndefined(initiationProcessedEventFromListener.replyMessageEvent);
                    }

                    done();
                });

                // First assert that there is no elements in 'initiations' before sending
                chai.assert.strictEqual(0, matsSocket.initiations.length);

                let receivedResolveInvoked = false;
                // Perform the requestReplyTo, which produces a Promise<ReceivedEvent>
                matsSocket.requestReplyTo("Test.slow", traceId, msg, "Test-terminator", undefined, {
                    timeout: replyTimeout
                }).then(receivedEvent => {
                    console.log("RECEIVED-PROMISE WAS RESOLVED! This should NOT happen!", receivedEvent);
                    receivedResolveInvoked = true;
                }).catch(receivedEvent => {
                    // The resolve should NOT have been invoked
                    chai.assert.isFalse(receivedResolveInvoked, "The Received-Promise should NOT have been invoked.");
                    // ORDERING: ReceivedEvent shall be first in line, before InitiationProcessedEvent and MessageEvent.
                    chai.assert.isUndefined(repliedMessageEvent, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced ReceivedEvent AFTER MessageEvent");
                    chai.assert.equal(matsSocket.initiations.length, 0, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced ReceivedEvent AFTER InitiationProcessedEvent (matsSocket.initiations)");
                    chai.assert.isUndefined(initiationProcessedEventFromListener, "Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced ReceivedEvent AFTER InitiationProcessedEvent (listeners)");

                    // The received roundTripTime should be equal to the one in InitiationProcessedEvent
                    receivedRoundTripMillisFromReceived = receivedEvent.roundTripMillis;
                });
            }

            it('requestReplyTo with timeout: (worst case wrt. event-issuing order) Event should be present on matsSocket.initiations, and be issued to listener (with replyTimeout=0, includeInitiationMessage, includeReplyMessageEvent=false).', function (done) {
                runRequestReplyToWithTimeoutTest(false, 0, done);
            });
            it('requestReplyTo with timeout: (worst case wrt. event-issuing order) Event should be present on matsSocket.initiations, and be issued to listener (with replyTimeout=0, with includeInitiationMessage, includeReplyMessageEvent=true).', function (done) {
                runRequestReplyToWithTimeoutTest(true, 0, done);
            });
            it('requestReplyTo with timeout: (worst case wrt. event-issuing order) Event should be present on matsSocket.initiations, and be issued to listener (with replyTimeout=25, includeInitiationMessage, includeReplyMessageEvent=false).', function (done) {
                runRequestReplyToWithTimeoutTest(false, 25, done);
            });
            it('requestReplyTo with timeout: (worst case wrt. event-issuing order) Event should be present on matsSocket.initiations, and be issued to listener (with replyTimeout=25, with includeInitiationMessage, includeReplyMessageEvent=true).', function (done) {
                runRequestReplyToWithTimeoutTest(true, 25, done);
            });
            it('requestReplyTo with timeout: (worst case wrt. event-issuing order) Event should be present on matsSocket.initiations, and be issued to listener (with replyTimeout=50, with includeInitiationMessage, includeReplyMessageEvent=false).', function (done) {
                runRequestReplyToWithTimeoutTest(false, 50, done);
            });
            it('requestReplyTo with timeout: (worst case wrt. event-issuing order) Event should be present on matsSocket.initiations, and be issued to listener (with replyTimeout=50, with includeInitiationMessage, includeReplyMessageEvent=true).', function (done) {
                runRequestReplyToWithTimeoutTest(true, 50, done);
            });

        });
    });
}));