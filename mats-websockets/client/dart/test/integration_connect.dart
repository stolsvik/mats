import 'dart:async';

import 'package:logging/logging.dart';
import 'package:mats_socket/mats_socket.dart';
import 'package:test/test.dart';
import 'lib/env.dart';

void main() {
  configureLogging();

  final _logger = Logger('integration_connect');

  group('MatsSocket integration tests, basics', () {
    MatsSocket matsSocket;

    void setAuth([String userId = 'standard',
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

    /*
         * NOTE!!! This test runs a scenario where two MatsSockets connect using the same MatsSocketSessionId. This is
         * NOT a situation that shall occur in actual use - you are NOT supposed to use the sessionId outside of the
         * MatsSocket instance - i.e. any given MatsSocket instance shall have a different sessionId than any other
         * MatsSocket instance.
         *
         * The test tries to emulate a situation where the MatsSocket client instance realizes that
         * the connection is broken, and starts to reconnect - while at the same time the Server has not realized this,
         * and thus still have the WebSocket connection open. In this situation, the Server will experience this as a
         * new WebSocket session firing up, and which tries to do a MatsSocket HELLO "Reconnect to existing SessionId",
         * while the Server still has a WebSocket session that already claims to be that MatsSocketSessionId. What the
         * Server is programmed to do, is to kill the existing socket with close code DISCONNECT, and then register the
         * existing MatsSocketSessionId to this new connection.
         *
         * What this test fails to actually exercise, is what happens if this scenario actually occurs *within a single
         * MatsSocket instance*: Does an outstanding Request that takes some time to finish actually resolve when the
         * situation has settled (i.e. when the new WebSocket connection has taken over, and the old killed)?
         *
         * If you have an idea of how to test this scenario in a better way, then please do so! Maybe using the unit
         * test harness?
         */
        group('connect twice to same MatsSocketSessionId', () {

            void connectTwice(Uri url1, Uri url2) async {
                // Create the first MatsSocket
                var matsSocket_A = MatsSocket('TestApp', '1.2.3', [url1]);
                var now = DateTime.now();
                var expiry = now.add(Duration(seconds: 20));
                matsSocket_A.setCurrentAuthorization('DummyAuth:standard:${expiry.millisecondsSinceEpoch}', expiry, Duration(seconds: 5));

                var matsSocket_A_SessionClosed = 0;
                matsSocket_A.addSessionClosedEventListener((closeEvent) {
                    matsSocket_A_SessionClosed++;
                });

                ConnectionEvent matsSocket_A_LostConnection;
                var matsSocket_A_LostConnection_Count = 0;
                matsSocket_A.addConnectionEventListener((connectionEvent) {
                    if (connectionEvent.type == ConnectionEventType.LOST_CONNECTION) {
                        matsSocket_A_LostConnection_Count++;
                        matsSocket_A_LostConnection = connectionEvent;
                    }
                });


                // Create a second MatsSocket, that will get the same MatsSocketSessionId as the first.
                var matsSocket_B = MatsSocket('TestApp', '1.2.3', [url2]);
                matsSocket_B.setCurrentAuthorization('DummyAuth:standard:${expiry.millisecondsSinceEpoch}', expiry, Duration(seconds: 5));

                var matsSocket_B_SessionClosed = 0;
                matsSocket_B.addSessionClosedEventListener((closeEvent) {
                    matsSocket_B_SessionClosed++;
                });

                var matsSocket_B_LostConnection_Count = 0;
                matsSocket_B.addConnectionEventListener((connectionEvent) {
                    if (connectionEvent.type == ConnectionEventType.LOST_CONNECTION) {
                        matsSocket_B_LostConnection_Count++;
                    }
                });

                // First SEND a message to instance A, so as to perform HELLO and get a WELCOME with sessionId
                var receivedEvent = await matsSocket_A.send('Test.ignoreInIncomingHandler', 'SEND_twiceConnect_ensureAssignedSessionId_${id(6)}', {});

                // Assert that the Resolve is MessageEventType.REPLY
                expect(receivedEvent.type, equals(ReceivedEventType.ACK));
                // Assert that the SessionClosedEvent listener was NOT invoked
                expect(matsSocket_A_SessionClosed, equals(0), reason: 'SessionClosedEvent listener should NOT have been invoked!');

                // Now, give this sessionId to instance B, and then send a REQUEST with this instance
                // This leads to HELLO->WELCOME, and that the server closes instance A with a DISCONNECT
                // NOTE: This shall happen both if the second connection is to the same node (server instance) as the first, or to a different node.

                // Do next step after a brief delay, so as to let our "delayed ACK/NACK/ACK2 compaction" work its magic.
                await Future.delayed(Duration(milliseconds: 50), () async {
                  // The MatsSocket has now gotten its SessionId, which we've sneakily made available on matsSocket_A.sessionId
                  // .. Set the existing sessionId on this new MatsSocket
                  matsSocket_B.sessionId = matsSocket_A.sessionId;

                  /* Now perform a request using matsSocket_B, which will "start" that MatsSocket instance,
                     * and hence perform a "HELLO(RECONNECT)" to the existing MatsSocketSessionId
                     * This should lead to the existing matsSocket_A being thrown out.
                     */
                  // :: Request to a MatsSocket->Mats service, ASAP reply
                  var req = {
                    'string': 'test twice connect',
                    'number': 42,
                    'sleepTime': 0
                  };
                  var receivedCallbackInvoked = 0;
                  var reply = await matsSocket_B.request(
                      'Test.simpleMats', 'REQUEST_twiceConnect_requestOnNewMatsSocket${id(6)}', req,
                      receivedCallback: (event) => receivedCallbackInvoked++);
                  var data = reply.data;
                  // Assert that we got receivedCallback ONCE
                  expect(receivedCallbackInvoked, equals(1), reason: 'Should have gotten one, and only one, receivedCallback.');
                  // Assert data, with the changes from Server side.
                  expect(data['string'], '${req['string']}:FromSimpleMats');
                  expect(data['number'], req['number']);
                  expect(data['sleepTime'], req['sleepTime']);
                });

                // Finally, assert lots of state and events that shall and shall not have happened.
                await Future.delayed(Duration(milliseconds: 150), () async {
                  // instance A should NOT be connected
                  expect(matsSocket_A.connected, isFalse, reason: 'Instance A should not be connected now!');
                  // .. but it should however still be in state ConnectionState.SESSION_ESTABLISHED (which is a bit weird, but think of the situation where this happens *within a single MatsSocket instance*.
                  expect(matsSocket_A.state, ConnectionState.SESSION_ESTABLISHED, reason: 'Instance A should still be in ConnectionState.SESSION_ESTABLISHED');
                  // .. and it should NOT have received a SessionClosedEvent (because the MatsSocketSession is NOT closed, it is just the WebSocket connection that has gone down).
                  expect(matsSocket_A_SessionClosed, 0, reason: 'SessionClosedEvent listener for instance A should NOT have been invoked!');
                  // .. but is SHOULD have received a single ConnectionEventType.LOST_CONNECTION (because that is definitely what has happened)
                  expect(matsSocket_A_LostConnection_Count, 1, reason: 'Instance A: ConnectionEventType.LOST_CONNECTION should have come, once');
                  // .. that has the webSocketEvent set.
                  expect(code(matsSocket_A_LostConnection), equals(MatsSocketCloseCodes.DISCONNECT.code), reason: 'Instance A: ConnectionEventType.LOST_CONNECTION should have webSocketEvent, and its code should be MatsSocketCloseCodes.DISCONNECT.');
                  expect(reason(matsSocket_A_LostConnection), contains('same MatsSocketSessionId'), reason: "Instance A: ConnectionEventType.LOST_CONNECTION should have webSocketEvent, and should say something 'same MatsSocketSessionId'.");

                  // instance B SHOULD be connected
                  expect(matsSocket_B.connected, isTrue, reason: 'Instance B should be connected now!');
                  // .. and it should have state ConnectionState.SESSION_ESTABLISHED
                  expect(matsSocket_B.state, ConnectionState.SESSION_ESTABLISHED, reason: 'Instance B should be in ConnectionState.SESSION_ESTABLISHED');
                  // .. and it should NOT have received a SessionClosedEvent
                  expect(matsSocket_B_SessionClosed, 0, reason: 'SessionClosedEvent listener for instance B should NOT have been invoked!');
                  // .. and it should NOT have received a ConnectionEventType.LOST_CONNECTION
                  expect(matsSocket_B_LostConnection_Count, 0, reason: 'Instance B: ConnectionEventType.LOST_CONNECTION should NOT have come');

                });

                // Now close instance B - after a brief delay, so as to let our "delayed ACK/NACK/ACK2 compaction" work its magic.
                await Future.delayed(Duration(milliseconds: 150), () async {
                  await matsSocket_B.close('Twice-connect test done - instance A!');

                  // .. which should NOT fire SessionClosed (since client side close())
                  expect(matsSocket_B_SessionClosed, 0, reason: 'SessionClosedEvent listener for instance B should NOT have been invoked!');
                  // .. but it should NOT be connected anymore
                  expect(matsSocket_B.connected, isFalse, reason: 'Instance A should not be connected now!');
                  // .. and state should be NO_SESSION
                  expect(matsSocket_B.state, ConnectionState.NO_SESSION, reason: 'Instance B should, after close, be in ConnectionState.NO_SESSION');

                  // Just to clean up, also close instance A - but this instance is pretty much dead anyway, and the MatsSocketSession is closed on server after close of instance B
                  await matsSocket_A.close('Twice-connect test done - instance A!');
                });
            }

            test('Connect twice to same Server - the second connection should result in the first being killed.', () async {
                await connectTwice(serverUris[0], serverUris[0]);
            });

            test('Connect twice to different Server - the second connection should result in the first being killed.', () async {
                await connectTwice(serverUris[0], serverUris[1]);
            });
        });
  });
}