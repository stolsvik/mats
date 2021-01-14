import 'dart:async';
import 'dart:convert';
import 'dart:math' as math;

import 'package:logging/logging.dart';
import 'package:mats_socket/mats_socket.dart';
import 'package:test/test.dart';

import 'lib/env.dart';

void main() {
  configureLogging();

  final _logger = Logger('integration_push');

  group('MatsSocket integration tests of Server-side send/request ("push")', () {
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

    setUp(() => matsSocket = createMatsSocket());

    tearDown(() async  {
      await matsSocket.close('Test done');
      _logger.info('=========== Closed MatsSocket [${matsSocket?.matsSocketInstanceId}] ===========');
    });

    group('MatsSocketServer.send()', () {
      // Set a valid authorization before each request
      setUp(setAuth);

      test('Send a message to Server, which responds by sending a message to terminator at Client (us!), directly in the MatsStage', () async {
        var traceId = 'MatsSocketServer.send_test_${id(6)}';
        var reply = matsSocket.terminator('ClientSide.terminator').first;
        await matsSocket.send('Test.server.send.matsStage', traceId, {'number': math.e});
        var msg = await reply;
        expect(msg.data['number'], equals(math.e));
        expect(msg.traceId, contains(':SentFromMatsStage'));
      });

      test('Send a message to Server, which responds by sending a message to terminator at Client (us!), in a separate Thread', () async {
        var traceId = 'MatsSocketServer.send_test_${id(6)}';
        var reply = matsSocket.terminator('ClientSide.terminator').first;

        await matsSocket.send('Test.server.send.thread', traceId, {'number': math.e});

        var msg = await reply;
        expect(msg.data['number'], equals(math.e));
        expect(msg.traceId, contains(':SentFromThread'));
      });
    });

    group('MatsSocketServer.request()', () {
      // Set a valid authorization before each request
      setUp(setAuth);

      Future doTest(String startEndpoint, bool resolveReject) async {
        var traceId = 'MatsSocketServer.send_test_${id(6)}';

        var initialMessage = 'Message_${id(20)}';

        // This endpoint will get a request from the Server, to which we respond - and the server will then send the reply back to the Terminator below.
        matsSocket.endpoint('ClientSide.endpoint', (messageEvent) {
          _logger.info('[sid:${matsSocket.sessionId}] We received a request to [ClientSide.endpoint], which we will [${resolveReject ? 'resolve' : 'reject'}], then wait for our terminator to be called');
          expect(messageEvent.traceId, equals(traceId));
          var promise = Completer();
          // Resolve it a tad later, to "emulate" some kind of processing
          Timer.run(() {
            var data = messageEvent.data;
            var msg = {'string': "${data['string']}:From_IntegrationEndpointA", 'number': data['number'] + math.pi};
            // ?: Resolve or Reject based on param
            if (resolveReject) {
              promise.complete(msg);
            } else {
              promise.completeError(msg);
            }
          });
          return promise.future;
        });

        // This terminator will get the final result from the Server
        var terminator = matsSocket.terminator('ClientSide.terminator');

        // Here we send the message that starts the cascade
        _logger.info('[sid:${matsSocket.sessionId}] Sending to [$startEndpoint], thus triggering the server to send a request');
        await matsSocket.send(startEndpoint, traceId, {'string': initialMessage, 'number': math.e});
        _logger.info('[sid:${matsSocket.sessionId}] Sending ACKed on server, the server should have sent a request, the client shall reply, and then the server should send to our terminator, which we are waiting for now');

        var msg = await terminator.first;
        _logger.info('[sid:${matsSocket.sessionId}] Terminator message received [$msg}], assrting content');
        expect(msg.data['number'], math.e + math.pi);
        expect(msg.data['string'], contains('From_IntegrationEndpointA'));
        expect(msg.data['string'], contains((resolveReject ? 'RESOLVE' : 'REJECT')));
        expect(msg.traceId, traceId);
      }

      test('Send a message to the Server, which responds by directly doing a Server-to-Client request (thus coming back here!), resolves, and when this returns to Server, sends it back to a Client Terminator', () async {
        await doTest('Test.server.request.direct', true);
      });

      test('Send a message to the Server, which responds by directly doing a Server-to-Client request (thus coming back here!), rejects, and when this returns to Server, sends it back to a Client Terminator', () async {
        await doTest('Test.server.request.direct', false);
      });

      test('Send a message to the Server, which responds by - in a Mats terminator - doing a Server-to-Client request (thus coming back here!), resolve, and when this returns to Server, sends it back to a Client Terminator', () async {
        await doTest('Test.server.request.viaMats', true);
      });

      test('Send a message to the Server, which responds by - in a Mats terminator - doing a Server-to-Client request (thus coming back here!), rejects, and when this returns to Server, sends it back to a Client Terminator', () async {
        await doTest('Test.server.request.viaMats', false);
      });
    });

    group('Server initiated sends and requests - with DebugOptions', () {
      void extracted(MessageEvent msg, int debugOptions) {
        // This is a server-initiated message, so these should be undefined in MessageEvent
        expect(msg.clientRequestTimestamp, isNull);
        expect(msg.roundTripMillis, isNull);

        // :: Assert debug
        // It should be present in MessageEvent, since it should be in the message from the server
        expect(msg.debug, isNotNull);
        // These should be set
        if ((debugOptions & DebugOption.TIMESTAMPS.flag) > 0) {
          expect(msg.debug.serverMessageCreated, isA<DateTime>());
          expect(msg.debug.messageSentToClient, isA<DateTime>());
        } else {
          expect(msg.debug.serverMessageCreated, isNull);
          expect(msg.debug.messageSentToClient, isNull);
        }
        if ((debugOptions & DebugOption.NODES.flag) > 0) {
          expect(msg.debug.serverMessageCreatedNodename, isNotNull);
          expect(msg.debug.messageSentToClientNodename, isNotNull);
        } else {
          expect(msg.debug.serverMessageCreatedNodename, isNull);
          expect(msg.debug.messageSentToClientNodename, isNull);
        }
        // While all other should not be set
        expect(msg.debug.clientMessageSent, isNull);
        expect(msg.debug.clientMessageReceived, isNull);
        expect(msg.debug.clientMessageReceivedNodename, isNull);
        expect(msg.debug.matsMessageSent, isNull);
        expect(msg.debug.matsMessageReplyReceived, isNull);
        expect(msg.debug.matsMessageReplyReceivedNodename, isNull);

        // This is not a request from Client, so there is no requested debug options.
        expect(msg.debug.requestedDebugOptions, isNull);
        // This is the resolved from what we asked server to use above, and what we are allowed to.
        expect(msg.debug.resolvedDebugOptions, equals(matsSocket.debug));
      }

      Future testServerInitiatedSend_DebugOptions(int debugOptions) async {
        // Set special userId that gives us all DebugOptions
        setAuth('enableAllDebugOptions');

        var traceId = 'MatsSocketServer.DebugOptions_server.send_${id(6)}';

        // These will become the server's initiation requested DebugOptions upon the subsequent 'send'
        matsSocket.debug = debugOptions;
        var terminator = matsSocket.terminator('ClientSide.terminator');
        await matsSocket.send('Test.server.send.thread', traceId, {'number': math.e});

        var msg = await terminator.first;
        expect(msg.data['number'], math.e);
        expect(msg.traceId, '$traceId:SentFromThread');

        extracted(msg, debugOptions);
      }

      test('Server initiated send should have debug object if user asks for it - with the all server-sent stuff filled.', () async {
        await testServerInitiatedSend_DebugOptions(DebugOption.NODES.flag | DebugOption.TIMESTAMPS.flag);
      });
      test('Server initiated send should have debug object if user asks for just nodes - with just the nodes filled.', () async {
        await testServerInitiatedSend_DebugOptions(DebugOption.NODES.flag);
      });
      test('Server initiated send should have debug object if user asks for just timestamps - with just the timestamps filled.', () async {
        await testServerInitiatedSend_DebugOptions(DebugOption.TIMESTAMPS.flag);
      });

      Future testServerInitiatedRequest_DebugOptions(int debugOptions) async {
        // Set special userId that gives us all DebugOptions
        setAuth('enableAllDebugOptions');

        var traceId = 'MatsSocketServer.DebugOptions_server.send_${id(6)}';

        // These will become the server's initiation requested DebugOptions upon the subsequent 'send'
        matsSocket.debug = debugOptions;
        var initialMessage = 'Message_${id(20)}';

        matsSocket.endpoint('ClientSide.endpoint', (msg) {
          expect(msg.data['number'], equals(math.e));
          expect(msg.traceId, equals(traceId));
          extracted(msg, debugOptions);

          return Future.value(
              {'string': "${msg.data['string']}:From_DebugOptions_test", 'number': msg.data['number'] + math.pi});
        });

        // This terminator will get the final result from the Server
        var terminator = matsSocket.terminator('ClientSide.terminator');

        await matsSocket.send('Test.server.request.direct', traceId, {'string': initialMessage, 'number': math.e});

        var msg = await terminator.first;
        expect(msg.data['number'], equals(math.e + math.pi));
        expect(msg.data['string'], equals('$initialMessage:From_DebugOptions_test:RESOLVE'));
        expect(msg.traceId, equals(traceId));
      }

      test('Server initiated request should have debug object if user asks for it - with the all server-sent stuff filled.', () async {
        await testServerInitiatedRequest_DebugOptions(DebugOption.NODES.flag | DebugOption.TIMESTAMPS.flag);
      });
      test('Server initiated request should have debug object if user asks for just nodes - with just the nodes filled.', () async {
        await testServerInitiatedRequest_DebugOptions(DebugOption.NODES.flag);
      });
      test('Server initiated request should have debug object if user asks for just timestamps - with just the timestamps filled.', () async {
        await testServerInitiatedRequest_DebugOptions(DebugOption.TIMESTAMPS.flag);
      });

      Future serverInitiatedSend_NoDebugOptions(int debugOptions) async {
        // Set special userId that gives us all DebugOptions
        setAuth('enableAllDebugOptions');

        var traceId = 'MatsSocketServer.DebugOptions_server.send_${id(6)}';
        var terminator = matsSocket.terminator('ClientSide.terminator');

        // :: These will become the server's initiation requested DebugOptions upon the subsequent 'send'
        // First set it to "all the things!"
        matsSocket.debug = 255;

        var first = matsSocket.send('Test.single', 'DebugOptions_set_to_all', {'number': math.e});

        // Then set it to none (0 or undefined), which should result in NO debug object from server-initiated messages
        matsSocket.debug = debugOptions;

        await matsSocket.send('Test.server.send.thread', traceId, {'number': math.e});

        var msg = await terminator.first;

        expect(msg.data['number'], equals(math.e));
        expect(msg.traceId, equals(traceId + ':SentFromThread'));

        // This is a server-initiated message, so these should be undefined in MessageEvent
        expect(msg.clientRequestTimestamp, isNull);
        expect(msg.roundTripMillis, isNull);

        // :: Assert debug
        // Since this is server-initiated, '0' will not give debug object - i.e. the server does not have difference between '0' and 'undefined'.
        expect(msg.debug, isNull);
      }

      test('Server initiated send should NOT have debug object if user ask for 0.', () async {
        await serverInitiatedSend_NoDebugOptions(0);
      });
      test('Server initiated send should NOT have debug object if user ask for \'undefined\'.', () async {
        await serverInitiatedSend_NoDebugOptions(null);
      });
    });
  });
}
