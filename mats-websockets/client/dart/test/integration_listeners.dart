import 'dart:async';

import 'package:logging/logging.dart';
import 'package:mats_socket/mats_socket.dart';
import 'package:mats_socket/src/InitiationProcessedEvent.dart';
import 'package:test/test.dart';
import 'lib/env.dart';
import 'dart:math' as math;

void main() {
  configureLogging();

  final _logger = Logger('integration_listeners');

  group('MatsSocket integration tests, listeners', () {
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

    // TODO: Check ConnectionEventListeners, including matsSocket.state

    // ===================================================================================
    // == NOTE: SessionClosedEventListener is tested in "connect, reconnect and close". ==
    // ===================================================================================

    group('PingPing listeners', () {
      // Set a valid authorization before each request
      setUp(setAuth);

      test('PingPong listener.', () async {
        var testCompleter = Completer();
        // matsSocket.initialPingDelay = 50;
        matsSocket.addPingPongListener((pingPong) {
          testCompleter.complete(() {
            expect(pingPong.pingId, equals(0));
            expect(pingPong.sentTimestamp, greaterThan(DateTime.fromMillisecondsSinceEpoch(1585856935097)));
            expect(pingPong.roundTripMillis,
                greaterThanOrEqualTo(Duration.zero)); // Might take 0.4ms, and Firefox will round that to 0.
          });
        });
        await matsSocket.send('Test.ignoreInIncomingHandler', 'PingPong_listener_${id(6)}', {});
        await testCompleter.future;
      });
    });

    group('InitiationProcessedEvent listeners', () {
      // Set a valid authorization before each request
      setUp(setAuth);

      Future runSendTest(bool includeInitiationMessage) async {
        var listenerProcessed = Completer();
        var traceId = 'InitiationProcessedEvent_send_${includeInitiationMessage}_${id(6)}';
        var msg = {'string': 'The String', 'number': math.pi};

        void assertCommon(InitiationProcessedEvent init) {
          expect(init.type, equals(InitiationProcessedEventType.SEND));
          expect(init.endpointId, equals('Test.single'));
          expect(init.sentTimestamp, greaterThan(DateTime.fromMillisecondsSinceEpoch(1585259649178)));
          expect(init.sessionEstablishedOffsetMillis, greaterThan(0.0),
              reason: 'sessionEstablishedOffsetMillis should be negative since sent before WELCOME, was [${init.sessionEstablishedOffsetMillis}]');
          expect(init.traceId, equals(traceId));
          expect(init.acknowledgeRoundTripMillis, greaterThan(0.0)); // Should probably take more than 1 ms.
          // These are undefined for 'send':
          expect(init.replyMessageEventType, isNull);
          expect(init.replyToTerminatorId, isNull);
          expect(init.requestReplyRoundTripMillis, isNull);
          expect(init.replyMessageEvent, isNull);
        }

        /*
                 *  This is a send. Order should be:
                 *  - ReceivedEvent, by settling the returned Received-Promise from invocation of 'send'.
                 *  - InitiationProcessedEvent on matsSocket.initiations
                 *  - InitiationProcessedEvent on listeners
                 */

        var receivedRoundTripMillisFromReceived;
        var initiationProcessedEventFromListener;

        matsSocket.addInitiationProcessedEventListener((processedEvent) {
          listenerProcessed.complete(() {
            // Assert common between matsSocket.initiations and listener event.
            assertCommon(processedEvent);

            // ReceivedEvent shall have been processed, as that is the first in order
            expect(receivedRoundTripMillisFromReceived, isNotNull,
                reason: 'Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}}: experienced InitiationProcessedEvent (listener) BEFORE ReceivedEvent');
            // The matsSocket.initiations should be there, as listeners are after
            expect(matsSocket.initiations.length, equals(1),
                reason: 'Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}}: experienced InitiationProcessedEvent (listener) BEFORE InitiationProcessedEvent (matsSocket.initiations)');

            // Assert common between matsSocket.initiations and listener event.
            assertCommon(matsSocket.initiations[0]);

            initiationProcessedEventFromListener = processedEvent;

            // ?: Is initiationMessage included?
            if (includeInitiationMessage) {
              // -> Yes, so then it should be here
              expect(processedEvent.initiationMessage, equals(msg));
            } else {
              // -> No, so then it should be undefined
              expect(processedEvent.initiationMessage, isNull);
            }

            var initiation = matsSocket.initiations[0];
            // On matsSocket.initiations, the initiationMessage should always be present.
            expect(initiation.initiationMessage, equals(msg));
            // The ackRoundTrip should again be equal to received.
            expect(initiation.acknowledgeRoundTripMillis, equals(receivedRoundTripMillisFromReceived));
          });
        }, includeInitiationMessage, false);

        // First assert that there is no elements in 'initiations' before sending
        expect(matsSocket.initiations, isEmpty);

        var receivedEvent = await matsSocket.send('Test.single', traceId, msg);

        // ORDERING: ReceivedEvent shall be first in line, before InitiationProcessedEvent and MessageEvent.
        expect(matsSocket.initiations, isEmpty,
            reason: 'Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}}: experienced ReceivedEvent AFTER InitiationProcessedEvent (matsSocket.initiations)');
        expect(initiationProcessedEventFromListener, isNull,
            reason: 'Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}}: experienced ReceivedEvent AFTER InitiationProcessedEvent (listeners)');

        // The received roundTripTime should be equal to the one in InitiationProcessedEvent
        receivedRoundTripMillisFromReceived = receivedEvent.roundTripMillis;

        await listenerProcessed.future;
      }

      test('send: event should be present on matsSocket.initiations, and be issued to listener (with includeInitiationMessage=false).', () async {
        await runSendTest(false);
      });
      test('send: event should be present on matsSocket.initiations, and be issued to listener (with includeInitiationMessage=true).', () async {
        await runSendTest(true);
      });

      Future runRequestTest(bool includeStash) async {
        var initiationProcessedEvent = Completer<InitiationProcessedEvent>.sync();
        var receivedCompleter = Completer<ReceivedEvent>.sync();
        var repliedMessageEvent = Completer<MessageEvent>.sync();
        var traceId = 'InitiationProcessedEvent_request_${id(6)}';
        var msg = {'string': 'The Strange', 'number': math.e};

        var receivedRoundTripMillisFromReceived;

        void assertCommon(InitiationProcessedEvent init) {
          expect(init.type, equals(InitiationProcessedEventType.REQUEST));
          expect(init.endpointId, 'Test.single');
          expect(init.sentTimestamp.millisecondsSinceEpoch, greaterThan(1585259649178));
          expect(init.sessionEstablishedOffsetMillis, lessThan(0.0),
              reason: 'sessionEstablishedOffsetMillis should be negative since sent before WELCOME, was [${init.sessionEstablishedOffsetMillis}]');
          expect(init.traceId, traceId);
          expect(init.acknowledgeRoundTripMillis, greaterThanOrEqualTo(1.0)); // Should probably take more than 1 ms.
          expect(init.acknowledgeRoundTripMillis, receivedRoundTripMillisFromReceived);
          expect(init.replyMessageEventType, equals(MessageEventType.RESOLVE));
          expect(init.requestReplyRoundTripMillis, greaterThanOrEqualTo(1.0)); // Should probably take more than 1 ms.
        }

        /*
         *  This is a 'request'. Order should be:
         *  - ReceivedEvent - invoked on receivedCallback supplied in 'request'-invocation.
         *  - InitiationProcessedEvent on matsSocket.initiations
         *  - InitiationProcessedEvent on listeners
         *  - MessageEvent, by settling the returned Reply-Promise from invocation of 'request'.
         */
        matsSocket.addInitiationProcessedEventListener(initiationProcessedEvent.complete, includeStash, includeStash);

        // First assert that there is no elements in 'initiations' before sending
        expect(0, matsSocket.initiations.length);

        // Perform the request, with a receivedCallback, which produces a Promise<MessageEvent>.
        matsSocket.request('Test.single', traceId, msg, receivedCallback: receivedCompleter.complete).then(repliedMessageEvent.complete);

        var receivedEvent = await receivedCompleter.future;
        // ORDERING: ReceivedEvent shall be first in line, before InitiationProcessedEvent and MessageEvent.
        expect(repliedMessageEvent.isCompleted, isFalse,
            reason: 'Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced ReceivedEvent AFTER MessageEvent');
        expect(matsSocket.initiations, isEmpty,
            reason: 'Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced ReceivedEvent AFTER InitiationProcessedEvent (matsSocket.initiations)');
        expect(initiationProcessedEvent.isCompleted, isFalse,
            reason: 'Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced ReceivedEvent AFTER InitiationProcessedEvent (listeners)');

        receivedRoundTripMillisFromReceived = receivedEvent.roundTripMillis;

        var processedEvent = await initiationProcessedEvent.future;
        // Assert common between matsSocket.initiations and listener event.
        assertCommon(processedEvent);

        // The matsSocket.initiations should be there, as listeners are after
        expect(matsSocket.initiations.length, equals(1),
            reason: 'Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced InitiationProcessedEvent (listener) BEFORE InitiationProcessedEvent (matsSocket.initiations)');
        // The MessageEvent shall not have come yet, we are earlier.
        expect(repliedMessageEvent.isCompleted, isFalse,
            reason: 'Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced InitiationProcessedEvent (listener) AFTER MessageEvent');

        assertCommon(matsSocket.initiations[0]);

        // ?: Is initiationMessage included?
        if (includeStash) {
          // -> Yes, so then it should be here
          expect(processedEvent.initiationMessage, msg);
        } else {
          // -> No, so then it should be undefined
          expect(processedEvent.initiationMessage, isNull);
        }

        var messageEvent = await repliedMessageEvent.future;

        // ReceivedEvent shall have been processed, as that is the first in order
        expect(receivedRoundTripMillisFromReceived, isNotNull,
            reason: 'Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced MessageEvent BEFORE ReceivedEvent');
        // There should now be ONE matsSocket.initiation already in place, as MessageEvent shall be the latest
        expect(matsSocket.initiations.length, equals(1),
            reason: 'Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced MessageEvent BEFORE InitiationProcessedEvent (matsSocket.initiations)');
        // The InitiationProcessedEvent listener should have been invoked, as MessageEvent shall be the latest
        expect(initiationProcessedEvent.isCompleted, isTrue,
            reason: 'Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced MessageEvent BEFORE InitiationProcessedEvent (listener)');

        var initiation = matsSocket.initiations[0];

        // Assert common between matsSocket.initiations and listener event.
        assertCommon(initiation);

        // This is not a requestReplyTo
        expect(initiation.replyToTerminatorId, isNull);
        // On matsSocket.initiations, the initiationMessage should always be present.
        expect(initiation.initiationMessage, msg);
        // On matsSocket.initiations, the replyMessageEvent should always be present.
        expect(initiation.replyMessageEvent, messageEvent);

        // ?: If we asked to include it for listener, then the replyMessageEvent should be the same object as we got here
        if (includeStash) {
          expect(processedEvent.replyMessageEvent, equals(messageEvent));
        } else {
          expect(processedEvent.replyMessageEvent, isNull);
        }
      }

      test('request: Event should be present on matsSocket.initiations, and be issued to listener (with includeInitiationMessage, includeReplyMessageEvent=false).', () async {
        await runRequestTest(false);
      });
      test('request: Event should be present on matsSocket.initiations, and be issued to listener (with includeInitiationMessage, includeReplyMessageEvent=true).', () async {
        await runRequestTest(true);
      });

      Future runRequestReplyToTest(bool includeStash) async {
        var initiationProcessedEvent = Completer<InitiationProcessedEvent>();
        var repliedMessageEvent = Completer<MessageEvent>();

        var traceId = 'InitiationProcessedEvent_requestReplyTo_${id(6)}';
        var msg = {'string': 'The Curious', 'number': math.pi};

        double receivedRoundTripMillisFromReceived;

        void assertCommon(InitiationProcessedEvent init) {
          expect(init.type, equals(InitiationProcessedEventType.REQUEST_REPLY_TO));
          expect(init.endpointId, equals('Test.single'));
          expect(init.sentTimestamp.millisecondsSinceEpoch, greaterThan(1585259649178));
          expect(init.sessionEstablishedOffsetMillis, lessThan(0.0),
              reason: 'sessionEstablishedOffsetMillis should be negative since sent before WELCOME, was [${init.sessionEstablishedOffsetMillis}]');
          expect(init.traceId, equals(traceId));
          expect(init.acknowledgeRoundTripMillis, greaterThanOrEqualTo(1.0)); // Should probably take more than 1 ms.
          expect(init.acknowledgeRoundTripMillis, receivedRoundTripMillisFromReceived);
          expect(init.replyMessageEventType, equals(MessageEventType.RESOLVE));
          expect(init.requestReplyRoundTripMillis, greaterThanOrEqualTo(1.0)); // Should probably take more than 1 ms.
        }

        matsSocket.addInitiationProcessedEventListener(initiationProcessedEvent.complete, includeStash, includeStash);

        matsSocket.terminator('Test-terminator').listen(repliedMessageEvent.complete, onError: repliedMessageEvent.completeError);


        /*
         *  This is a 'requestReplyTo'. Order should be:
         *  - ReceivedEvent - on returned Received-Promise from invocation of 'requestReplyTo'
         *  - InitiationProcessedEvent on matsSocket.initiations
         *  - InitiationProcessedEvent on listeners
         *  - MessageEvent, sent to Terminator by invocation of its 'messageCallback'
         */

        // Perform the requestReplyTo, which produces a Promise<ReceivedEvent>
        var receivedEvent = await matsSocket.requestReplyTo('Test.single', traceId, msg, 'Test-terminator');
        // ORDERING: ReceivedEvent shall be first in line, before InitiationProcessedEvent and MessageEvent.
        expect(repliedMessageEvent.isCompleted, isFalse, reason: 'Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced ReceivedEvent AFTER MessageEvent');
        expect(matsSocket.initiations.length, equals(0), reason: 'Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced ReceivedEvent AFTER InitiationProcessedEvent (matsSocket.initiations)');
        expect(initiationProcessedEvent.isCompleted, isFalse, reason: 'Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced ReceivedEvent AFTER InitiationProcessedEvent (listeners)');


        // The received roundTripTime should be equal to the one in InitiationProcessedEvent
        receivedRoundTripMillisFromReceived = receivedEvent.roundTripMillis;

        var processedEvent = await initiationProcessedEvent.future;

        // Assert common between matsSocket.initiations and listener event.
        assertCommon(processedEvent);

        // The matsSocket.initiations should be there, as listeners are after
        expect(matsSocket.initiations.length, equals(1), reason: 'Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced InitiationProcessedEvent (listener) BEFORE InitiationProcessedEvent (matsSocket.initiations)');
        // The MessageEvent shall not have come yet, we are earlier.
        expect(repliedMessageEvent.isCompleted, isFalse, reason: 'Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced InitiationProcessedEvent (listener) AFTER MessageEvent');

        assertCommon(matsSocket.initiations[0]);

        // ?: Is initiationMessage included?
        if (includeStash) {
            // -> Yes, so then it should be here
            expect(processedEvent.initiationMessage, equals(msg));
        } else {
            // -> No, so then it should be undefined
            expect(processedEvent.initiationMessage, isNull);
        }

        var messageEvent = await repliedMessageEvent.future;

        var initiation = matsSocket.initiations[0];

        // Assert common between matsSocket.initiations and listener event.
        assertCommon(initiation);

        // This IS a requestReplyTo
        expect(initiation.replyToTerminatorId, equals('Test-terminator'));
        // On matsSocket.initiations, the initiationMessage should always be present.
        expect(initiation.initiationMessage, equals(msg));
        // On matsSocket.initiations, the replyMessageEvent should always be present.
        expect(initiation.replyMessageEvent, equals(messageEvent));

        // ?: If we asked to include it for listener, then the replyMessageEvent should be the same object as we got here
        if (includeStash) {
            expect(processedEvent.replyMessageEvent, messageEvent);
        } else {
            expect(processedEvent.replyMessageEvent, isNull);
        }
      }

      test('requestReplyTo: Event should be present on matsSocket.initiations, and be issued to listener (with includeInitiationMessage, includeReplyMessageEvent=false).', () async {
        await runRequestReplyToTest(false);
      });
      test('requestReplyTo: Event should be present on matsSocket.initiations, and be issued to listener (with includeInitiationMessage, includeReplyMessageEvent=true).', () async {
        await runRequestReplyToTest(true);
      });

      Future runRequestReplyToWithTimeoutTest(bool includeStash, int replyTimeout) async {
        var initiationProcessedEvent = Completer<InitiationProcessedEvent>();
        var repliedMessageEvent = Completer<MessageEvent>();

        var traceId = 'InitiationProcessedEvent_requestReplyToWithTimeout_${id(6)}';
        var msg = {
          'string': 'The Straight-Out Frightening',
          'number': math.sqrt2,
          'sleepIncoming': 150, // Sleeptime before acknowledging - longer than the timeout.
          'sleepTime': 10 // Sleeptime before replying
        };

        var receivedRoundTripMillisFromReceived;

        void assertCommon(InitiationProcessedEvent init) {
          expect(init.type, equals(InitiationProcessedEventType.REQUEST_REPLY_TO));
          expect(init.endpointId, equals('Test.slow'));
          expect(init.sentTimestamp.millisecondsSinceEpoch, greaterThan(1585259649178));
          // Note: Will be zero if session is not established yet.
          expect(init.sessionEstablishedOffsetMillis, lessThanOrEqualTo(0.0),
              reason: 'sessionEstablishedOffsetMillis should be zero or negative, since sent before WELCOME, was [${init.sessionEstablishedOffsetMillis}]');
          expect(init.traceId, equals(traceId));
          // Evidently, we sometimes get 0 ms - probably only when doing timeout with 0 ms in this test.
          expect(init.acknowledgeRoundTripMillis, greaterThanOrEqualTo(0.0));
          expect(init.acknowledgeRoundTripMillis, equals(receivedRoundTripMillisFromReceived));
          expect(init.replyMessageEventType, equals(MessageEventType.TIMEOUT));
          // Might sometimes get 0 ms - probably only when doing timeout with 0 ms in this test.
          expect(init.requestReplyRoundTripMillis, greaterThanOrEqualTo(0.0)); // Evidently, we sometimes get 0 ms
        }

        /*
         *  This is a 'requestReplyTo'. Order should be:
         *  - ReceivedEvent - on returned Received-Promise from invocation of 'requestReplyTo'
         *  - InitiationProcessedEvent on matsSocket.initiations
         *  - InitiationProcessedEvent on listeners
         *  - MessageEvent, sent to Terminator by invocation of its 'messageCallback'
         */

        matsSocket.addInitiationProcessedEventListener(initiationProcessedEvent.complete, includeStash, includeStash);

        var messageCallbackInvoked = false;
        await matsSocket.terminator('Test-terminator').listen((messageEvent) {
          fail('TERMINATOR WAS RESOLVED (messageCallback!) - this should NOT happen!');
        }, onError: repliedMessageEvent.complete);

        // First assert that there is no elements in 'initiations' before sending
        expect(matsSocket.initiations, isEmpty);

        // Perform the requestReplyTo, which produces a Promise<ReceivedEvent>
        try {
          await matsSocket.requestReplyTo('Test.slow', traceId, msg, 'Test-terminator', timeout: Duration(milliseconds: replyTimeout));
          fail('RECEIVED-PROMISE WAS RESOLVED! This should NOT happen!');
        } on ReceivedEvent catch(receivedEvent) {
          // ORDERING: ReceivedEvent shall be first in line, before InitiationProcessedEvent and MessageEvent.
          expect(repliedMessageEvent.isCompleted, isFalse,
              reason: 'Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced ReceivedEvent AFTER MessageEvent');
          expect(matsSocket.initiations, isEmpty,
              reason: 'Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced ReceivedEvent AFTER InitiationProcessedEvent (matsSocket.initiations)');
          expect(initiationProcessedEvent.isCompleted, isFalse,
              reason: 'Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced ReceivedEvent AFTER InitiationProcessedEvent (listeners)');

          // The received roundTripTime should be equal to the one in InitiationProcessedEvent
          receivedRoundTripMillisFromReceived = receivedEvent.roundTripMillis;
        }

        var processedEvent = await initiationProcessedEvent.future;
        // Assert common between matsSocket.initiations and listener event.
        assertCommon(processedEvent);

        // ReceivedEvent shall have been processed, as that is the first in order
        expect(receivedRoundTripMillisFromReceived, isNotNull,
            reason: 'Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced InitiationProcessedEvent (listener) BEFORE ReceivedEvent');
        // The matsSocket.initiations should be there, as listeners are after
        expect(matsSocket.initiations.length, equals(1),
            reason: 'Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced InitiationProcessedEvent (listener) BEFORE InitiationProcessedEvent (matsSocket.initiations)');
        // The MessageEvent shall not have come yet, we are earlier.
        expect(repliedMessageEvent.isCompleted, isFalse,
            reason: 'Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced InitiationProcessedEvent (listener) AFTER MessageEvent');

        assertCommon(matsSocket.initiations[0]);

        // ?: Is initiationMessage included?
        if (includeStash) {
          // -> Yes, so then it should be here
          expect(processedEvent.initiationMessage, msg);
        } else {
          // -> No, so then it should be undefined
          expect(processedEvent.initiationMessage, isNull);
        }

        var messageEvent = await repliedMessageEvent.future;
        // The messageCallback shall not have been invoked
        expect(messageCallbackInvoked, isFalse, reason: 'messageCallback should NOT have been invoked');

        // ReceivedEvent shall have been processed, as that is the first in order
        expect(receivedRoundTripMillisFromReceived, isNotNull, reason: 'Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced MessageEvent BEFORE ReceivedEvent');
        // There should now be ONE matsSocket.initiation already in place, as MessageEvent shall be the latest
        expect(matsSocket.initiations.length, equals(1), reason: 'Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced MessageEvent BEFORE InitiationProcessedEvent (matsSocket.initiations)');
        // The InitiationProcessedEvent listener should have been invoked, as MessageEvent shall be the latest
        expect(initiationProcessedEvent.isCompleted, isTrue, reason: 'Violated ordering, shall be: {ReceivedEvent, InitiationProcessedEvent {matsSocket.initiations, then listeners}, MessageEvent}: experienced MessageEvent BEFORE InitiationProcessedEvent (listener)');

        var initiation = matsSocket.initiations[0];

        // Assert common between matsSocket.initiations and listener event.
        assertCommon(initiation);

        // This IS a requestReplyTo
        expect(initiation.replyToTerminatorId, 'Test-terminator');
        // On matsSocket.initiations, the initiationMessage should always be present.
        expect(initiation.initiationMessage, msg);
        // On matsSocket.initiations, the replyMessageEvent should always be present.
        expect(initiation.replyMessageEvent, messageEvent);

        // ?: If we asked to include it for listener, then the replyMessageEvent should be the same object as we got here
        if (includeStash) {
          expect(processedEvent.replyMessageEvent, messageEvent);
        } else {
          expect(processedEvent.replyMessageEvent, isNull);
        }

      }

      test('requestReplyTo with timeout: (worst case wrt. event-issuing order) Event should be present on matsSocket.initiations, and be issued to listener (with replyTimeout=0, includeInitiationMessage, includeReplyMessageEvent=false).', () async {
        await runRequestReplyToWithTimeoutTest(false, 0);
      });
      test('requestReplyTo with timeout: (worst case wrt. event-issuing order) Event should be present on matsSocket.initiations, and be issued to listener (with replyTimeout=0, with includeInitiationMessage, includeReplyMessageEvent=true).', () async {
        await runRequestReplyToWithTimeoutTest(true, 0);
      });
      test('requestReplyTo with timeout: (worst case wrt. event-issuing order) Event should be present on matsSocket.initiations, and be issued to listener (with replyTimeout=25, includeInitiationMessage, includeReplyMessageEvent=false).', () async {
        await runRequestReplyToWithTimeoutTest(false, 25);
      });
      test('requestReplyTo with timeout: (worst case wrt. event-issuing order) Event should be present on matsSocket.initiations, and be issued to listener (with replyTimeout=25, with includeInitiationMessage, includeReplyMessageEvent=true).', () async {
        await runRequestReplyToWithTimeoutTest(true, 25);
      });
      test('requestReplyTo with timeout: (worst case wrt. event-issuing order) Event should be present on matsSocket.initiations, and be issued to listener (with replyTimeout=50, with includeInitiationMessage, includeReplyMessageEvent=false).', () async {
        await runRequestReplyToWithTimeoutTest(false, 50);
      });
      test('requestReplyTo with timeout: (worst case wrt. event-issuing order) Event should be present on matsSocket.initiations, and be issued to listener (with replyTimeout=50, with includeInitiationMessage, includeReplyMessageEvent=true).', () async {
        await runRequestReplyToWithTimeoutTest(true, 50);
      });
    });
  });
}
