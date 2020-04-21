import 'dart:async';

import 'package:logging/logging.dart';
import 'package:mats_socket/mats_socket.dart';
import 'package:test/test.dart';
import 'lib/env.dart';
import 'dart:math' as math;

void main() {
  configureLogging();

  final _logger = Logger('integration_basic');

  group('MatsSocket integration tests, basics', () {
    MatsSocket matsSocket;

    void setAuth(
        [String userId = 'standard',
          Duration duration = const Duration(seconds: 20),
          roomForLatencyMillis = const Duration(seconds: 10)]) {
      var now = DateTime.now();
      var expiry = now.add(duration);
      matsSocket.setCurrentAuthorization(
          'DummyAuth:$userId:${expiry.millisecondsSinceEpoch}', expiry, roomForLatencyMillis);
    }

    setUp(() {
      matsSocket = MatsSocket('TestApp', '1.2.3', serverUris);
      _logger.info('Created MatsSocket instance [${matsSocket.matsSocketInstanceId}]');
    });

    tearDown(() async  {
      await matsSocket.close('Test done');
    });

    void standardStateAssert() {
      expect(matsSocket.connected, isTrue, reason: 'MatsSocket has been closed, which was not expected here!');
      expect(matsSocket.state, equals(ConnectionState.SESSION_ESTABLISHED),
          reason: 'MatsSocket should have been in ConnectionState.SESSION_ESTABLISHED!');
    }

    group('simple sends', () {
      // Set a valid authorization before each request
      setUp(setAuth);

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

      test('Should have a promise that resolves when received', () async {
        // Return a promise, that mocha will watch and resolve
        await matsSocket.send('Test.single', 'SEND_${id(6)}', {'string': 'The String', 'number': math.pi});
      });

      test('Send to non-existing endpoint should NACK from Server and reject Promise', () async {
        var promise =
            matsSocket.send('NON EXISTING!', 'SEND_NonExisting${id(6)}', {'string': 'The String', 'number': math.pi});
        await promise.catchError((receivedEvent) {
          standardStateAssert();
          expect(receivedEvent.type, equals(ReceivedEventType.NACK));
        });
      });
    });

    group('request', () {
      // Set a valid authorization before each request
      setUp(setAuth);

      test('Request should resolve Promise', () async {
        // Return a promise, that mocha will watch and resolve
        await matsSocket
            .request('Test.single', 'REQUEST-with-Promise_${id(6)}', {'string': 'Request String', 'number': math.e});
      });

      test('Request should invoke both the ack callback and resolve Promise', () async {
        var received = false;
        await matsSocket.request('Test.single', 'REQUEST-with-Promise-and-receivedCallback_${id(6)}', {},
            receivedCallback: (event) => received = true);
        standardStateAssert();
        expect(received, isTrue, reason: 'The received-callback should have been invoked.');
      });

      test('Request to non-existing endpoint should NACK from Server, invoking receivedCallback and reject Promise',
          () async {
        expect(
            matsSocket.request('NON EXISTING!', 'REQUEST_NonExisting${id(6)}', {}, receivedCallback: (event) {
              standardStateAssert();
            }),
            throwsA(isA<MessageEvent>()));
      });
    });

    group('requestReplyTo', () {
      // Set a valid authorization before each request
      setUp(setAuth);

      test('Should reply to our own endpoint', () async {
        var terminator = matsSocket.terminator('ClientSide.testTerminator');
        await matsSocket.requestReplyTo('Test.single', 'REQUEST-with-ReplyTo_${id(6)}',
            {'string': 'Request String', 'number': math.e}, 'ClientSide.testTerminator');

        await terminator.first;
      });

      test('Should reply to our own endpoint with our correlation data', () async {
        var correlationInformation = id(5);
        var terminator = matsSocket.terminator('ClientSide.testTerminator');
        await matsSocket.requestReplyTo('Test.single', 'REQUEST-with-ReplyTo_withCorrelationInfo_${id(6)}',
            {'string': 'Request String', 'number': math.e}, 'ClientSide.testTerminator',
            correlationInformation: correlationInformation);

        var msg = await terminator.first;
        expect(msg.correlationInformation, equals(correlationInformation));

        standardStateAssert();
      });
    });

    group('timeout on request', () {
      // Set a valid authorization before each request
      setUp(setAuth);

      test('request with timeout earlier than slow endpoint', () async {
        var testCompleter = Completer();
        // :: The actual test
        void test() async {
          var req = {
            'string': 'test',
            'number': -1,
            'sleepIncoming': 50, // Sleeptime before acknowledging
            'sleepTime': 50 // Sleeptime before replying
          };
          var receivedCallbackInvoked = false;
          var reply = matsSocket.request('Test.slow', 'Timeout_request_${id(6)}', req,
              // Low timeout to NOT get ReceivedEventType.ACK.
              timeout: Duration(milliseconds: 10), receivedCallback: (event) {
            expect(event.type, equals(ReceivedEventType.TIMEOUT));
            receivedCallbackInvoked = true;
          }).catchError((event) {
            standardStateAssert();
            expect(event.type, equals(MessageEventType.TIMEOUT));
            if (receivedCallbackInvoked) {
              testCompleter.complete();
            }
            return event;
          });
          matsSocket.flush();
        }

        // :: .. but must first wait for SESSION_ESTABLISHED to run test
        matsSocket.addConnectionEventListener((event) {
          if (event.type == ConnectionEventType.SESSION_ESTABLISHED) {
            standardStateAssert();
            test();
          }
        });
        // MatsSocket does not start until first message.
        await matsSocket.send('Test.ignoreInIncomingHandler', 'Timeout_request_start', {});
        await testCompleter.future;
      });

      test('requestReplyTo with timeout earlier than slow endpoint', () async {
        var correlationInformation = jid(100);
        var testCompleter = Completer();

        /**
         * NOTICE: We should FIRST get the rejection on the Received Promise from requestReplyTo, and THEN
         * we should get a reject on the Terminator.
         */

        var receivedResolveInvoked = false;
        var receivedRejectInvoked = false;

        // :: The actual test
        var test = () {
          var req = {
            'string': 'test',
            'number': -2,
            'sleepIncoming': 50, // Sleeptime before acknowledging
            'sleepTime': 50 // Sleeptime before replying
          };
          matsSocket
              .requestReplyTo('Test.slow', 'Timeout_requestReplyTo_${id(6)}', req, 'Test.terminator',
                  correlationInformation: correlationInformation, timeout: Duration(milliseconds: 5))
              .then((_) => receivedResolveInvoked = true, onError: (event) {
            standardStateAssert();
            expect(event.type, equals(ReceivedEventType.TIMEOUT));
            receivedRejectInvoked = true;
          });
          matsSocket.flush();
        };

        // Create Terminator to receive the return
        var messageCallbackInvoked = false;
        var firstMessage = matsSocket.terminator("Test.terminator").first.then((messageEvent) {
          // We do NOT expect a message!
          messageCallbackInvoked = true;
        }, onError: (event) {
          standardStateAssert();
          expect(event.type, equals(MessageEventType.TIMEOUT));
          expect(event.correlationInformation, equals(correlationInformation));
          // NOTE! The ReceivedReject should already have been invoked.
          expect(receivedRejectInvoked, isTrue);

          // Just wait a tad to see that we don't also get the messageCallbackInvoked
          Timer.run(() {
            testCompleter.complete(() {
              standardStateAssert();
              expect(receivedResolveInvoked, isFalse);
              expect(messageCallbackInvoked, isFalse);
            });
          });
        });

        // :: .. but must first wait for SESSION_ESTABLISHED to run test
        matsSocket.addConnectionEventListener((event) {
          if (event.type == ConnectionEventType.SESSION_ESTABLISHED) {
            standardStateAssert();
            test();
          }
        });
        // MatsSocket does not start until first message.
        await matsSocket.send('Test.ignoreInIncomingHandler', 'Timeout_requestReplyTo_start', {});
        await firstMessage;
        await testCompleter.future;
      });
    });

    group('pipeline', () {
      var testCompleter = Completer();

      // Set a valid authorization before each request
      setUp(setAuth);

      test('Pipeline should reply to our specified Terminator', () async {
        var replyCount = 0;
        matsSocket.terminator('ClientSide.testEndpoint').listen((e) {
          standardStateAssert();
          replyCount += 1;
          if (replyCount == 3) {
            testCompleter.complete();
          }
        });

        // These three will be "autopipelined".

        var req1 = matsSocket.requestReplyTo(
            'Test.single',
            'REQUEST-with-ReplyTo_Pipeline_1_${id(6)}',
            {'string': 'Messge 1', 'number': 100.001, 'requestTimestamp': DateTime.now().millisecondsSinceEpoch},
            'ClientSide.testEndpoint',
            correlationInformation: 'pipeline_1_${id(10)}');
        var req2 = matsSocket.requestReplyTo(
            'Test.single',
            'REQUEST-with-ReplyTo_Pipeline_2_${id(6)}',
            {'string': 'Message 2', 'number': 200.002, 'requestTimestamp': DateTime.now().millisecondsSinceEpoch},
            'ClientSide.testEndpoint',
            correlationInformation: 'pipeline_2_${id(10)}');
        var req3 = matsSocket.requestReplyTo(
            'Test.single',
            'REQUEST-with-ReplyTo_Pipeline_3_${id(6)}',
            {'string': 'Message 3', 'number': 300.003, 'requestTimestamp': DateTime.now().millisecondsSinceEpoch},
            'ClientSide.testEndpoint',
            correlationInformation: 'pipeline_3_${id(10)}');
        matsSocket.flush();

        // Wait for the last future, to ensure all processing done, and that the test completer is
        // triggered.
        await Stream.fromFutures([req1, req2, req3, testCompleter.future]).last;
      });
    });

    group('requests handled in IncomingAuthorizationAndAdapter', () {
      // Set a valid authorization before each request
      setUp(setAuth);

      // FOR ALL: Both the received callback should be invoked, and the Promise resolved/rejected

      test(
          'Ignored (handler did nothing), should NACK when REQUEST (and thus reject Promise) since it is not allowed to ignore a Request (must either deny, insta-settle or forward)',
          () async {
        var received = false;
        var promise = matsSocket.request(
            'Test.ignoreInIncomingHandler', 'REQUEST_ignored_in_incomingHandler${id(6)}', {},
            receivedCallback: (event) => received = true);
        await promise.catchError((event) {
          standardStateAssert();
          expect(received, isTrue, reason: "The received-callback should have been invoked.");
          return event;
        });
      });

      test('context.deny() should NACK (and thus reject Promise)', () async {
        var received = false;
        var promise = matsSocket.request('Test.denyInIncomingHandler', 'REQUEST_denied_in_incomingHandler${id(6)}', {},
            receivedCallback: (event) => received = true);
        await promise.catchError((event) {
          standardStateAssert();
          expect(received, isTrue, reason: 'The received-callback should have been invoked.');
        });
      });

      test('context.resolve(..) should ACK received, and RESOLVE the Promise.', () async {
        var received = false;
        var promise = matsSocket.request(
            'Test.resolveInIncomingHandler', 'REQUEST_resolved_in_incomingHandler${id(6)}', {},
            receivedCallback: (event) => received = true);
        await promise.then((event) {
          standardStateAssert();
          expect(received, isTrue, reason: 'The received-callback should have been invoked.');
        });
      });

      test('context.reject(..) should ACK received, and REJECT the Promise', () async {
        var received = false;
        var promise = matsSocket.request(
            'Test.rejectInIncomingHandler', 'REQUEST_rejected_in_incomingHandler${id(6)}', {},
            receivedCallback: (event) => received = true);
        await promise.catchError((event) {
          standardStateAssert();
          expect(received, isTrue, reason: 'The received-callback should have been invoked.');
        });
      });

      test('Exception in incomingAdapter should NACK (and thus reject Promise)', () async {
        var received = false;
        var promise = matsSocket.request(
            'Test.throwsInIncomingHandler', 'REQUEST_throws_in_incomingHandler${id(6)}', {},
            receivedCallback: (event) => received = true);
        await promise.catchError((event) {
          standardStateAssert();
          expect(received, isTrue, reason: 'The received-callback should have been invoked.');
        });
      });
    });

    group('sends handled in IncomingAuthorizationAndAdapter', () {
      // Set a valid authorization before each request
      setUp(setAuth);

      // FOR ALL: Both the received callback should be invoked, and the Promise resolved/rejected

      test('Ignored (handler did nothing) should ACK when SEND (thus resolve Promise)', () async {
        await matsSocket.send('Test.ignoreInIncomingHandler', 'SEND_ignored_in_incomingHandler${id(6)}', {});
        standardStateAssert();
      });

      test('context.deny() should NACK (reject Promise)', () async {
        var promise = matsSocket.send('Test.denyInIncomingHandler', 'SEND_denied_in_incomingHandler${id(6)}', {});
        await promise.catchError((_) {
          standardStateAssert();
        });
      });

      test('context.resolve(..) should NACK (reject Promise) since it is not allowed to resolve/reject a send',
          () async {
        var promise = matsSocket.send('Test.resolveInIncomingHandler', 'SEND_resolved_in_incomingHandler${id(6)}', {});
        await promise.catchError((_) {
          standardStateAssert();
        });
      });

      test('context.reject(..) should NACK (reject Promise) since it is not allowed to resolve/reject a send',
          () async {
        var promise = matsSocket.send('Test.rejectInIncomingHandler', 'SEND_rejected_in_incomingHandler${id(6)}', {});
        await promise.catchError((_) {
          standardStateAssert();
        });
      });

      test('Exception in incomingAdapter should NACK (reject Promise)', () async {
        var promise = matsSocket.send('Test.throwsInIncomingHandler', 'SEND_throws_in_incomingHandler${id(6)}', {});
        await promise.catchError((_) {});
      });
    });

    group('requests handled in replyAdapter', () {
      // Set a valid authorization before each request
      setUp(setAuth);

      // FOR ALL: Both the received callback should be invoked, and the Promise resolved/rejected

      test(
          'Ignored (handler did nothing) should NACK when Request handled in adaptReply(..) (thus nack receivedCallback, and reject Promise)',
          () async {
        var received = false;
        var rejected = false;
        await matsSocket.request('Test.ignoreInReplyAdapter', 'REQUEST_ignored_in_replyAdapter${id(6)}', {},
            receivedCallback: (event) {
          standardStateAssert();
          received = true;
        }).catchError((event) {
          rejected = true;
          return event;
        });

        standardStateAssert();
        expect(received, isTrue);
        expect(rejected, isTrue);
      });

      test('context.resolve(..)', () async {
        var received = false;
        await matsSocket.request('Test.resolveInReplyAdapter', 'REQUEST_resolved_in_replyAdapter${id(6)}', {},
            receivedCallback: (event) {
          standardStateAssert();
          received = true;
        });

        standardStateAssert();
        expect(received, isTrue);
      });

      test('context.reject(..)', () async {
        var received = false;
        var rejected = false;
        await matsSocket.request('Test.rejectInReplyAdapter', 'REQUEST_rejected_in_replyAdapter${id(6)}', {},
            receivedCallback: (event) {
          standardStateAssert();
          received = true;
        }).catchError((event) {
          rejected = true;
          return event;
        });

        standardStateAssert();
        expect(received, isTrue);
        expect(rejected, isTrue);
      });

      test('Exception in replyAdapter should reject', () async {
        var received = false;
        var rejected = false;

        await matsSocket.request('Test.throwsInReplyAdapter', 'REQUEST_throws_in_replyAdapter${id(6)}', {},
            receivedCallback: (event) {
          standardStateAssert();
          received = true;
        }).catchError((event) {
          rejected = true;
          return event;
        });

        standardStateAssert();
        expect(received, isTrue);
        expect(rejected, isTrue);
      });
    });

    group('debug object', () {
      Future<DebugDto> testDebugOptionsSend({int debug, String userId = 'enableAllDebugOptions'}) async {
        // Set special userId that gives us all DebugOptions
        setAuth(userId);

        var receivedEventReceived;
        var messageEvent = await matsSocket
            .request('Test.single', 'Request_DebugObject_${debug}_${matsSocket.debug}_${id(6)}', {},
                receivedCallback: (receivedEvent) {
          standardStateAssert();
          receivedEventReceived = receivedEvent;
        }, debug: debug);
        standardStateAssert();
        expect(receivedEventReceived, isNotNull);
        expect(messageEvent.debug, isNotNull);

        return messageEvent.debug;
      }

      void assertNodes(DebugDto debug) {
        expect(debug.clientMessageReceivedNodename, isA<String>());
        expect(debug.matsMessageReplyReceivedNodename, isA<String>());
        expect(debug.messageSentToClientNodename, isA<String>());
      }

      void assertNodesUndefined(DebugDto debug) {
        expect(debug.clientMessageReceivedNodename, isNull);
        expect(debug.matsMessageReplyReceivedNodename, isNull);
        expect(debug.messageSentToClientNodename, isNull);
      }

      void assertTimestamps(DebugDto debug) {
        expect(debug.clientMessageSent, isA<DateTime>());
        expect(debug.clientMessageReceived, isA<DateTime>());
        expect(debug.matsMessageSent, isA<DateTime>());
        expect(debug.matsMessageReplyReceived, isA<DateTime>());
        expect(debug.messageSentToClient, isA<DateTime>());
        expect(debug.messageReceived, isA<DateTime>());
      }

      void assertTimestampsFromServerAreUndefined(DebugDto debug) {
        expect(debug.clientMessageSent, isA<DateTime>());
        expect(debug.clientMessageReceived, isNull);
        expect(debug.matsMessageSent, isNull);
        expect(debug.matsMessageReplyReceived, isNull);
        expect(debug.messageSentToClient, isNull);
        expect(debug.messageReceived, isA<DateTime>());
      }

      test(
          'When the user is allowed to debug, and request all via matsSocket.debug, the debug object should be present and all filled',
          () async {
        matsSocket.debug = DebugOption.NODES.flag | DebugOption.TIMESTAMPS.flag;
        var debug = await testDebugOptionsSend();
        assertNodes(debug);
        assertTimestamps(debug);
      });

      test(
          'When the user is allowed to debug, and request DebugOption.NODES via matsSocket.debug, the debug object should be present and filled with just nodes, not timestamps',
          () async {
        matsSocket.debug = DebugOption.NODES.flag;
        var debug = await testDebugOptionsSend();
        assertNodes(debug);
        assertTimestampsFromServerAreUndefined(debug);
      });

      test(
          'When the user is allowed to debug, and request DebugOption.TIMINGS via matsSocket.debug, the debug object should be present and filled with just timestamps, not nodes',
          () async {
        matsSocket.debug = DebugOption.TIMESTAMPS.flag;
        var debug = await testDebugOptionsSend();
        assertNodesUndefined(debug);
        assertTimestamps(debug);
      });

      test(
          "When the user is allowed to debug, and request all via message-specific config, while matsSocket.debug='undefined', the debug object should be present and all filled",
          () async {
        matsSocket.debug = null;
        var debug = await testDebugOptionsSend(debug: DebugOption.NODES.flag | DebugOption.TIMESTAMPS.flag);
        assertNodes(debug);
        assertTimestamps(debug);
      });

      test(
          'When the user is NOT allowed to debug, and request all via matsSocket.debug, the debug object should just have the Client-side filled stuff',
          () async {
        matsSocket.debug = DebugOption.NODES.flag | DebugOption.TIMESTAMPS.flag;
        var debug = await testDebugOptionsSend(userId: 'userWithoutDebugOptions');
        assertNodesUndefined(debug);
        assertTimestampsFromServerAreUndefined(debug);
      });

      test(
          'When the user is allowed to debug, but do not request any debug (undefined), there should not be a debug object',
          () async {
        // Set special userId that gives us all DebugOptions
        setAuth('enableAllDebugOptions');

        var receivedEventReceived;
        var messageEvent = await matsSocket.request('Test.single', 'Request_DebugObject_non_requested_${id(6)}', {},
            receivedCallback: (receivedEvent) {
          standardStateAssert();
          receivedEventReceived = receivedEvent;
        });
        standardStateAssert();
        expect(receivedEventReceived, isNotNull);
        expect(messageEvent.debug, isNull);
      });

      test('When the user is allowed to debug, but do not request any debug (0), there should not be a debug object',
          () async {
        // Set special userId that gives us all DebugOptions
        setAuth('enableAllDebugOptions');

        matsSocket.debug = 0;

        var receivedEventReceived;
        var messageEvent = await matsSocket.request('Test.single', 'Request_DebugObject_non_requested_${id(6)}', {},
            receivedCallback: (receivedEvent) {
          standardStateAssert();
          receivedEventReceived = receivedEvent;
        });
        standardStateAssert();
        expect(receivedEventReceived, isNotNull);
        expect(messageEvent.debug, isNull);
      });
    });
  });
}
