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

    describe('MatsSocket integration-tests', function () {
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

        describe('reconnect', function () {
            it('reconnects and completes outstanding request when invoking reconnect()', function (done) {
                setAuth();

                let firstTime = true;

                let killSocket = function(connectionEvent) {
                   if (firstTime && (connectionEvent.state === mats.ConnectionState.SESSION_ESTABLISHED)) {
                       matsSocket.reconnect("Integration-test, testing reconnects");
                       firstTime = false;
                   }
                };

                matsSocket.addConnectionEventListener(killSocket);

                // Request to a service that won't reply immediately.
                matsSocket.request("Test.slow", "REQUEST_reconnect1_" + matsSocket.id(6), {
                    sleepTime: 350
                }).then(reply => {
                    done();
                });
            });


            // TODO: Test reconnect with different userId.
            // TODO: Test two MatsSockets to same SessionId - the first one should be closed. Which close code?
        });

        describe('simple sends', function () {
            // Set a valid authorization before each request
            beforeEach(() => setAuth());

            // NOTE: There used to be a "fire-and-forget" variant here. The problem is that when matsSocket.close() is
            // invoked, it rejects all outstanding messages - and since this happens earlier than the acknowledge actually
            // coming back, Node gets angry with the following information:
            //
            // (node:30412) UnhandledPromiseRejectionWarning: Unhandled promise rejection. This error originated either by
            //              throwing inside of an async function without a catch block, or by rejecting a promise which was
            //              not handled with .catch(). (rejection id: 1)
            // (node:30412) [DEP0018] DeprecationWarning: Unhandled promise rejections are deprecated. In the future,
            //              promise rejections that are not handled will terminate the Node.js process with a non-zero exit
            //              code.

            it('Should have a promise that resolves when received', function () {
                // Return a promise, that mocha will watch and resolve
                return matsSocket.send("Test.single", "SEND_" + matsSocket.id(6), {
                    string: "The String",
                    number: Math.PI
                });
            });

            it('Send to non-existing endpoint should NACK from Server and reject Promise', function (done) {
                let promise = matsSocket.send("NON EXISTING!", "SEND_NonExisting" + matsSocket.id(6), {
                    string: "The String",
                    number: Math.PI
                });
                promise.catch(reason => {
                    chai.assert(matsSocket.connected, "MatsSocket has been closed, which was not expected here!");
                    done();
                });
            });
        });

        describe("request", function () {
            // Set a valid authorization before each request
            beforeEach(() => setAuth());

            it("Request should resolve Promise", function () {
                // Return a promise, that mocha will watch and resolve
                return matsSocket.request("Test.single", "REQUEST-with-Promise_" + matsSocket.id(6), {
                    string: "Request String",
                    number: Math.E
                })
            });

            it("Request should invoke both the ack callback and resolve Promise", function (done) {
                let received = false;
                let promise = matsSocket.request("Test.single", "REQUEST-with-Promise-and-receivedCallback_" + matsSocket.id(6), {},
                    function () {
                        received = true;
                    });
                promise.then(reason => {
                    chai.assert(received, "The received-callback should have been invoked.");
                    chai.assert(matsSocket.connected, "MatsSocket has been closed, which was not expected here!");
                    done();
                });
            });

            it('Request to non-existing endpoint should NACK from Server, invoking receivedCallback and reject Promise', function (done) {
                let received = false;
                let promise = matsSocket.request("NON EXISTING!", "REQUEST_NonExisting" + matsSocket.id(6), {},
                    function () {
                        received = true;
                    });
                promise.catch(reason => {
                    chai.assert(received, "The received-callback should have been invoked.");
                    chai.assert(matsSocket.connected, "MatsSocket has been closed, which was not expected here!");
                    done();
                });
            });

        });

        describe("requestReplyTo", function () {
            // Set a valid authorization before each request
            beforeEach(() => setAuth());

            it("Should reply to our own endpoint", function (done) {
                matsSocket.terminator("ClientSide.customEndpoint", () => done());
                matsSocket.requestReplyTo("Test.single", "REQUEST-with-ReplyTo_1_" + matsSocket.id(6), {
                    string: "Request String",
                    number: Math.E
                }, "ClientSide.customEndpoint")
            });

            it("Should reply to our own endpoint with our correlation data", function (done) {
                const correlationId = matsSocket.id(6);
                matsSocket.terminator("ClientSide.customEndpoint", ({correlationId: messageCorrelationId}) => {
                    chai.assert.equal(messageCorrelationId, correlationId);
                    done()
                });
                matsSocket.requestReplyTo("Test.single", "REQUEST-with-ReplyTo_2_" + matsSocket.id(6), {
                    string: "Request String",
                    number: Math.E
                }, "ClientSide.customEndpoint", correlationId)
            });
        });

        describe("pipeline", function () {
            // Set a valid authorization before each request
            beforeEach(() => setAuth());

            it("Pipeline should reply to our own endpoint", function (done) {
                let replyCount = 0;
                matsSocket.terminator("ClientSide.testEndpoint", function (e) {
                    replyCount += 1;
                    if (replyCount === 3) {
                        done();
                    }
                });

                // These three will be "autopipelined".

                matsSocket.requestReplyTo("Test.single", "REQUEST-with-ReplyTo_Pipeline_1_" + matsSocket.id(6),
                    {string: "Messge 1", number: 100.001, requestTimestamp: Date.now()},
                    "ClientSide.testEndpoint", "pipeline_1_" + matsSocket.id(10));
                matsSocket.requestReplyTo("Test.single", "REQUEST-with-ReplyTo_Pipeline_2_" + matsSocket.id(6),
                    {string: "Message 2", number: 200.002, requestTimestamp: Date.now()},
                    "ClientSide.testEndpoint", "pipeline_2_" + matsSocket.id(10));
                matsSocket.requestReplyTo("Test.single", "REQUEST-with-ReplyTo_Pipeline_3_" + matsSocket.id(6),
                    {string: "Message 3", number: 300.003, requestTimestamp: Date.now()},
                    "ClientSide.testEndpoint", "pipeline_3_" + matsSocket.id(10));
                matsSocket.flush();
            });
        });

        describe("requests handled in IncomingAuthorizationAndAdapter", function () {
            // Set a valid authorization before each request
            beforeEach(() => setAuth());

            // FOR ALL: Both the received callback should be invoked, and the Promise resolved/rejected

            it("ignored (handler did nothing), should NACK when REQUEST (and thus reject Promise) since it is not allowed to ignore a Request (must either deny, insta-settle or forward)", function (done) {
                let received = false;
                let promise = matsSocket.request("Test.ignoreInIncomingHandler", "REQUEST_ignored_in_incomingHandler" + matsSocket.id(6), {},
                    function () {
                        received = true;
                    });
                promise.catch(function () {
                    chai.assert(received, "The received-callback should have been invoked.");
                    chai.assert(matsSocket.connected, "MatsSocket has been closed, which was not expected here!");
                    done();
                })
            });

            it("context.deny() should NACK (and thus reject Promise)", function (done) {
                let received = false;
                let promise = matsSocket.request("Test.denyInIncomingHandler", "REQUEST_denied_in_incomingHandler" + matsSocket.id(6), {},
                    function () {
                        received = true;
                    });
                promise.catch(function () {
                    chai.assert(received, "The received-callback should have been invoked.");
                    chai.assert(matsSocket.connected, "MatsSocket has been closed, which was not expected here!");
                    done();
                })
            });

            it("context.resolve(..) should ACK received, and RESOLVE the Promise.", function (done) {
                let received = false;
                let promise = matsSocket.request("Test.resolveInIncomingHandler", "REQUEST_resolved_in_incomingHandler" + matsSocket.id(6), {},
                    function () {
                        received = true;
                    });
                promise.then(function () {
                    chai.assert(received, "The received-callback should have been invoked.");
                    chai.assert(matsSocket.connected, "MatsSocket has been closed, which was not expected here!");
                    done();
                })

            });

            it("context.reject(..) should ACK received, and REJECT the Promise", function (done) {
                let received = false;
                let promise = matsSocket.request("Test.rejectInIncomingHandler", "REQUEST_rejected_in_incomingHandler" + matsSocket.id(6), {},
                    function () {
                        received = true;
                    });
                promise.catch(function () {
                    chai.assert(received, "The received-callback should have been invoked.");
                    chai.assert(matsSocket.connected, "MatsSocket has been closed, which was not expected here!");
                    done();
                });
            });

            it("Exception in incomingAdapter should NACK (and thus reject Promise)", function (done) {
                let received = false;
                let promise = matsSocket.request("Test.throwsInIncomingHandler", "REQUEST_throws_in_incomingHandler" + matsSocket.id(6), {},
                    function () {
                        received = true;
                    });
                promise.catch(function () {
                    chai.assert(received, "The received-callback should have been invoked.");
                    chai.assert(matsSocket.connected, "MatsSocket has been closed, which was not expected here!");
                    done();
                });
            });
        });

        describe("sends handled in IncomingAuthorizationAndAdapter", function () {
            // Set a valid authorization before each request
            beforeEach(() => setAuth());

            // FOR ALL: Both the received callback should be invoked, and the Promise resolved/rejected

            it("ignored (handler did nothing) should ACK when SEND (thus resolve Promise)", function (done) {
                let promise = matsSocket.send("Test.ignoreInIncomingHandler", "SEND_ignored_in_incomingHandler" + matsSocket.id(6), {});
                promise.then(function () {
                    chai.assert(matsSocket.connected, "MatsSocket has been closed, which was not expected here!");
                    done();
                })
            });

            it("context.deny() should NACK (reject Promise)", function (done) {
                let promise = matsSocket.send("Test.denyInIncomingHandler", "SEND_denied_in_incomingHandler" + matsSocket.id(6), {});
                promise.catch(function () {
                    chai.assert(matsSocket.connected, "MatsSocket has been closed, which was not expected here!");
                    done();
                })
            });

            it("context.resolve(..) should NACK (reject Promise) since it is not allowed to resolve/reject a send", function (done) {
                let promise = matsSocket.send("Test.resolveInIncomingHandler", "SEND_resolved_in_incomingHandler" + matsSocket.id(6), {});
                promise.catch(function () {
                    chai.assert(matsSocket.connected, "MatsSocket has been closed, which was not expected here!");
                    done();
                })

            });

            it("context.reject(..) should NACK (reject Promise) since it is not allowed to resolve/reject a send", function (done) {
                let promise = matsSocket.send("Test.rejectInIncomingHandler", "SEND_rejected_in_incomingHandler" + matsSocket.id(6), {});
                promise.catch(function () {
                    chai.assert(matsSocket.connected, "MatsSocket has been closed, which was not expected here!");
                    done();
                });
            });

            it("Exception in incomingAdapter should NACK (reject Promise)", function (done) {
                let promise = matsSocket.send("Test.throwsInIncomingHandler", "SEND_throws_in_incomingHandler" + matsSocket.id(6), {});
                promise.catch(function () {
                    chai.assert(matsSocket.connected, "MatsSocket has been closed, which was not expected here!");
                    done();
                });
            });
        });

        // TODO: Check ConnectionEventListeners, including matsSocket.state
        // TODO: Check SessionClosedEventListener


        describe("requests handled in replyAdapter", function () {
            // Set a valid authorization before each request
            beforeEach(() => setAuth());

            // FOR ALL: Both the received callback should be invoked, and the Promise resolved/rejected

            it("context.resolve(..)", function (done) {
                let received = false;
                let promise = matsSocket.request("Test.resolveInReplyAdapter", "REQUEST_resolved_in_replyAdapter" + matsSocket.id(6), {},
                    function () {
                        received = true;
                    });
                promise.then(function () {
                    if (received) {
                        done();
                    }
                });
            });

            it("context.reject(..)", function (done) {
                let received = false;
                let promise = matsSocket.request("Test.rejectInReplyAdapter", "REQUEST_rejected_in_replyAdapter" + matsSocket.id(6), {},
                    function () {
                        received = true;
                    });
                promise.catch(function () {
                    if (received) {
                        done();
                    }
                });
            });

            it("Exception in replyAdapter should reject", function (done) {
                let received = false;
                let promise = matsSocket.request("Test.throwsInReplyAdapter", "REQUEST_throws_in_replyAdapter" + matsSocket.id(6), {},
                    function () {
                        received = true;
                    });
                promise.catch(function () {
                    if (received) {
                        done();
                    }
                });
            })
            ;
        });

        describe("client close", function () {
            // Set a valid authorization before each request
            beforeEach(() => setAuth());

            it("send: Promise should reject if closed before ack", function (done) {
                let promise = matsSocket.send("Test.single", "SEND_should_reject_on_close_" + matsSocket.id(6), {
                    string: "The String",
                    number: Math.PI
                });
                promise.catch(function (event) {
                    done();
                });
                matsSocket.close("testing close rejects");
            });

            it("request: Promise should reject if closed before ack", function (done) {
                let promise = matsSocket.request("Test.single", "REQUEST_should_reject_on_close_" + matsSocket.id(6), {
                    string: "The String",
                    number: Math.PI
                });
                promise.catch(function (event) {
                    done();
                });
                matsSocket.close("testing close rejects");
            });
        });
    });
}));