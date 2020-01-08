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
    });

    afterEach(() => {
        matsSocket.closeSession("Test done");
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

        it('Should fire and forget', function () {
            matsSocket.send("Test.single", "SEND_" + matsSocket.id(6), {string: "The String", number: Math.PI});
        });

        it('Should create a promise that resolves', function () {
            // Return a promise, that mocha will watch and resolve
            return matsSocket.send("Test.single", "SEND_" + matsSocket.id(6), {string: "The String", number: Math.PI});
        });

    })

    describe("request", function () {
        // Set a valid authorization before each request
        beforeEach(() => setAuth());

        it("Should reply", function() {
            // Return a promise, that mocha will watch and resolve
            return matsSocket.request("Test.single", "REQUEST-with-Promise_" + matsSocket.id(6), {string: "Request String", number: Math.E})
        });

        it("Should invoke the ack callback", function (done) {
            matsSocket.request("Test.single", "REQUEST-with-Promise-and-receivedCallback_" + matsSocket.id(6), {string: "Request String", number: Math.E},
                // Call the done method when we receive the ack
                () => done());
        })

    });

    describe("requestReplyTo", function () {
        // Set a valid authorization before each request
        beforeEach(() => setAuth());

        it("Should reply to our own endpoint", function(done) {
            matsSocket.endpoint("ClientSide.customEndpoint", () => done());
            // Return a promise, that mocha will watch and resolve
            matsSocket.requestReplyTo("Test.single", "REQUEST-with-Promise_" + matsSocket.id(6), {string: "Request String", number: Math.E}, "ClientSide.customEndpoint")
        });

        it("Should reply to our own endpoint with our correlation data", function(done) {
            const correlationId = matsSocket.id(6)
            matsSocket.endpoint("ClientSide.customEndpoint", ({correlationId: messageCorrelationId}) => {
                assert.equal(messageCorrelationId, correlationId);
                done()
            });
            // Return a promise, that mocha will watch and resolve
            matsSocket.requestReplyTo("Test.single", "REQUEST-with-Promise_" + matsSocket.id(6), {string: "Request String", number: Math.E}, "ClientSide.customEndpoint", correlationId)
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


});
