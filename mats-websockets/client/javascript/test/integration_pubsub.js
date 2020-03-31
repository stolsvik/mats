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

    let logging = false;

    let matsSocket;

    function createMatsSocket() {
        matsSocket = new MatsSocket("TestApp", "1.2.3", availableUrls);
        matsSocket.logging = logging;
    }

    function closeMatsSocket() {
        // :: Chill the close slightly, so as to get the final "ACK2" envelope over to delete server's inbox.
        // NOTE: This is done async, so for the fast tests, the closings will come in "behind".
        let toClose = matsSocket;
        setTimeout(function () {
            toClose.close("Test done");
        }, 120);
    }

    function setAuth(userId = "standard", duration = 20000, roomForLatencyMillis = 10000) {
        const now = Date.now();
        const expiry = now + duration;
        matsSocket.setCurrentAuthorization("DummyAuth:" + userId + ":" + expiry, expiry, roomForLatencyMillis);
    }

    const availableUrls = (env.MATS_SOCKET_URLS || "ws://localhost:8080/matssocket,ws://localhost:8081/matssocket").split(",");


    describe('MatsSocket integration tests of "pub/sub" - Publish and Subscribe', function () {

        describe('reconnect', function () {
            // Create Socket before each request
            beforeEach(() => {
                createMatsSocket();
            });
            afterEach(() => {
                closeMatsSocket();
            });


            it('Sub/Pub - preliminary.', function (done) {
                setAuth();
                matsSocket.subscribe("Test.topic", function(messageEvent) {
                    done();
                });

                matsSocket.send("Test.publish", "PUBLISH_testSend"+matsSocket.id(5), "Testmessage");
            });
        });
    });
}));