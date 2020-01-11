const assert = require('assert');
const {MatsSocket} = require('../lib/MatsSocket');

describe('MatsSocket', function () {
    let matsSocket;

    function setAuth(duration = 20000, roomForLatencyMillis = 10000) {
        const now = Date.now();
        const expiry = now + duration;
        matsSocket.setCurrentAuthorization("DummyAuth:" + expiry, expiry, roomForLatencyMillis);
    }

    const urls = process.env.MATS_SOCKET_URLS || "ws://localhost:8080/matssocket/json,ws://localhost:8081/matssocket/json";

    beforeEach(() => {
        matsSocket = new MatsSocket("TestApp", "1.2.3", urls.split(","));
        // matsSocket.logging = true;
    });

    afterEach(() => {
        matsSocket.close("Test done");
    });

    describe('authorization', function () {
        it('Should invoke authorization callback before making calls', function(done) {
            let authCallbackCalled = false;

            matsSocket.setAuthorizationExpiredCallback(function (event) {
                authCallbackCalled = true;
                setAuth();
            });
            matsSocket.send("Test.single", "SEND_" + matsSocket.id(6), {}).then(reply => {
                assert(authCallbackCalled);
                done();
            });
        });

        it('Should not invoke authorization callback if authorization present', function(done) {
            let authCallbackCalled = false;
            setAuth();
            matsSocket.setAuthorizationExpiredCallback(function (event) {
                authCallbackCalled = true;
            });
            matsSocket.send("Test.single", "SEND_" + matsSocket.id(6), {}).then(reply => {
                assert(!authCallbackCalled);
                done();
            });
        });

        it('Should invoke authorization callback when expired', function(done) {
            let authCallbackCalled = false;

            setAuth(-20000);
            matsSocket.setAuthorizationExpiredCallback(function (event) {
                authCallbackCalled = true;
                setAuth();
            });
            matsSocket.send("Test.single", "SEND_" + matsSocket.id(6), {}).then(reply => {
                assert(authCallbackCalled);
                done();
            });

        });

        it('Should invoke authorization callback when room for latency expired', function(done) {
            let authCallbackCalled = false;

            setAuth(1000, 10000);
            matsSocket.setAuthorizationExpiredCallback(function (event) {
                authCallbackCalled = true;
                setAuth();
            });
            matsSocket.send("Test.single", "SEND_" + matsSocket.id(6), {}).then(reply => {
                assert(authCallbackCalled);
                done();
            })
        });
    });

    describe('send', function () {
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

        it('Should create a promise that resolves', function () {
            // Return a promise, that mocha will watch and resolve
            return matsSocket.send("Test.single", "SEND_" + matsSocket.id(6), {string: "The String", number: Math.PI});
        });
    });

    describe("request", function () {
        // Set a valid authorization before each request
        beforeEach(() => setAuth());

        it("Should reply", function() {
            // Return a promise, that mocha will watch and resolve
            return matsSocket.request("Test.single", "REQUEST-with-Promise_" + matsSocket.id(6), {string: "Request String", number: Math.E})
        });

        it("Should invoke the ack callback", function () {
            // :: Need to make sure both the ack-callback, AND the reply-Promise resolves, otherwise the
            // reply-Promise rejects due to matsSocket.close() happening earlier than the actual reply, which Node hates.

            // This will hold the ordinary reply-Promise
            let replyPromise;
            // Create a new Promise for the ack-callback
            let ackCallbackPromise = new Promise(function(resolve, reject) {
                replyPromise = matsSocket.request("Test.single", "REQUEST-with-Promise-and-receivedCallback_" + matsSocket.id(6), {
                        string: "Request String",
                        number: Math.E
                    },
                    // Resolve the ackCallbackPromise when ack-callback is invoked
                    (e) => resolve(e));
            });

            // Create a new Promise that is resolved when both the ack-callback-Promise and the reply Promise is resolved.
            return ackCallbackPromise.then(function(result) {
                // console.log("Ack callback result: "+JSON.stringify(result));
                return replyPromise;
            });
        })
    });

    describe("requestReplyTo", function () {
        // Set a valid authorization before each request
        beforeEach(() => setAuth());

        it("Should reply to our own endpoint", function(done) {
            matsSocket.endpoint("ClientSide.customEndpoint", () => done());
            // Return a promise, that mocha will watch and resolve
            matsSocket.requestReplyTo("Test.single", "REQUEST-with-ReplyTo_1_" + matsSocket.id(6), {string: "Request String", number: Math.E}, "ClientSide.customEndpoint")
        });

        it("Should reply to our own endpoint with our correlation data", function(done) {
            const correlationId = matsSocket.id(6)
            matsSocket.endpoint("ClientSide.customEndpoint", ({correlationId: messageCorrelationId}) => {
                assert.equal(messageCorrelationId, correlationId);
                done()
            });
            // Return a promise, that mocha will watch and resolve
            matsSocket.requestReplyTo("Test.single", "REQUEST-with-ReplyTo_2_" + matsSocket.id(6), {string: "Request String", number: Math.E}, "ClientSide.customEndpoint", correlationId)
        });
    });

    describe("pipeline", function () {
        // Set a valid authorization before each request
        beforeEach(() => setAuth());

        it("Pipeline should reply to our own endpoint", function(done) {
            let replyCount = 0;
            matsSocket.endpoint("ClientSide.testEndpoint", function (e) {
                replyCount += 1;
                if (replyCount == 3) {
                    done();
                }
            });

            matsSocket.pipeline(function (ms) {
                ms.requestReplyTo("Test.single", "REQUEST-with-ReplyTo_Pipeline_1_" + matsSocket.id(6),
                    {string: "Messge 1", number: 100.001, requestTimestamp: Date.now()},
                    "ClientSide.testEndpoint", "pipeline_1_" + matsSocket.id(10));
                ms.requestReplyTo("Test.single", "REQUEST-with-ReplyTo_Pipeline_2_" + matsSocket.id(6),
                    {string: "Message 2", number: 200.002, requestTimestamp: Date.now()},
                    "ClientSide.testEndpoint", "pipeline_2_" + matsSocket.id(10));
                ms.requestReplyTo("Test.single", "REQUEST-with-ReplyTo_Pipeline_3_" + matsSocket.id(6),
                    {string: "Message 3", number: 300.003, requestTimestamp: Date.now()},
                    "ClientSide.testEndpoint", "pipeline_3_" + matsSocket.id(10));
            });
        });
    });

    describe("client close", function() {
        // Set a valid authorization before each request
        beforeEach(() => setAuth());

        it("send: Promise should reject if closed before ack", function(done) {
            let promise = matsSocket.send("Test.single", "SEND_should_reject_on_close_" + matsSocket.id(6), {string: "The String", number: Math.PI});
            matsSocket.close("testing close rejects");
            promise.catch(function(event) {
                done();
            });
        });

        it("request: Promise should reject if closed before ack", function(done) {
            let promise = matsSocket.request("Test.single", "REQUEST_should_reject_on_close_" + matsSocket.id(6), {string: "The String", number: Math.PI});
            matsSocket.close("testing close rejects");
            promise.catch(function(event) {
                done();
            });
        });
    });
});
