import 'dart:async';
import 'dart:math' as math;

import 'package:logging/logging.dart';
import 'package:mats_socket/mats_socket.dart';
import 'package:test/test.dart';

import 'lib/env.dart';

void main() {
  configureLogging();

  final _logger = Logger('integration_authentication');

  group('MatsSocket integration-tests of Authentication & Authorization', () {
    MatsSocket matsSocket;

    void setAuth(
        [String userId = 'standard',
        Duration duration = const Duration(seconds: 200),
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

    group('MatsSocket integration tests of Authentication & Authorization', () {
      group('authorization callbacks', () {
        test('Should invoke authorization callback before making calls', () async {
          var authCallbackCalled = false;
          matsSocket.setAuthorizationExpiredCallback((event) {
            authCallbackCalled = true;
            setAuth();
          });
          await matsSocket.send('Test.single', 'SEND_${id(6)}', {});
          expect(authCallbackCalled, isTrue);
        });

        test('Should not invoke authorization callback if authorization present', () async {
          var authCallbackCalled = false;
          setAuth();
          matsSocket.setAuthorizationExpiredCallback((event) {
            authCallbackCalled = true;
          });
          await matsSocket.send('Test.single', 'SEND_${id(6)}', {});
          expect(authCallbackCalled, isFalse);
        });

        test('Should invoke authorization callback when expired', () async {
          var authCallbackCalled = false;
          setAuth('standard', Duration(seconds: -20000));
          matsSocket.setAuthorizationExpiredCallback((event) {
            authCallbackCalled = true;
            setAuth();
          });
          await matsSocket.send('Test.single', 'SEND_${id(6)}', {});
          expect(authCallbackCalled, isTrue);
        });

        test('Should invoke authorization callback when room for latency expired', () async {
          var authCallbackCalled = false;
          // Immediately timed out.
          setAuth('standard', Duration(seconds: 1), Duration(seconds: 10));
          matsSocket.setAuthorizationExpiredCallback((event) {
            authCallbackCalled = true;
            setAuth();
          });
          await matsSocket.send('Test.single', 'SEND_${id(6)}', {});
          expect(authCallbackCalled, isTrue);
        });
      });

      group('authorization invalid when Server about to receive or send out information bearing message', () {
        Future testIt(userId) async {
          setAuth(userId, Duration(seconds: 2), Duration.zero);

          var authCallbackCalledCount = 0;
          AuthorizationRequiredEventType authCallbackCalledEventType;
          matsSocket.setAuthorizationExpiredCallback((event) {
            authCallbackCalledCount++;
            authCallbackCalledEventType = event.type;
            // This standard auth does not fail reevaluateAuthentication.
            setAuth();
          });
          var req = {'string': 'test', 'number': math.e, 'sleepTime': 0};
          var receivedCallbackInvoked = 0;
          await matsSocket
              .request('Test.slow', 'REQUEST_authentication_from_server_${id(6)}', req,
                  receivedCallback: (event) => receivedCallbackInvoked++)
              .then((reply) {
            var data = reply.data;
            // Assert that we got receivedCallback ONCE
            expect(receivedCallbackInvoked, equals(1),
                reason: 'Should have gotten one, and only one, receivedCallback.');
            // Assert that we got AuthorizationExpiredEventType.REAUTHENTICATE, and only one call to Auth.
            expect(authCallbackCalledEventType, equals(AuthorizationRequiredEventType.REAUTHENTICATE),
                reason:
                    'Should have gotten AuthorizationRequiredEventType.REAUTHENTICATE authorizationExpiredCallback.');
            expect(authCallbackCalledCount, 1,
                reason: 'authorizationExpiredCallback should only have been invoked once');
            // Assert data, with the changes from Server side.
            expect(data['string'], equals('${req['string']}:FromSlow'));
            expect(data['number'], equals(req['number']));
            expect(data['sleepTime'], equals(req['sleepTime']));
          });
        }

        test('Receive: Using special userId which DummyAuthenticator fails on step reevaluateAuthentication(..), Server shall ask for REAUTH when we perform Request, and when gotten, resolve w/o retransmit (server "holds" message).', () async {
          // Using special "userId" for DummySessionAuthenticator that specifically fails @ reevaluateAuthentication(..) step
          await testIt('fail_reevaluateAuthentication');
        });

        test('Reply from Server: Using special userId which DummyAuthenticator fails on step reevaluateAuthenticationForOutgoingMessage(..), Server shall require REAUTH from Client before sending Reply.', () async {
          // Using special "userId" for DummySessionAuthenticator that specifically fails @ reevaluateAuthenticationForOutgoingMessage(..) step
          await testIt('fail_reevaluateAuthenticationForOutgoingMessage');
        });
      });

      group('Server side invokes "magic" Client-side endpoint MatsSocket.renewAuth', () {
        test('MatsSocket.renewAuth: This endpoint forces invocation of authorizationExpiredCallback, the reply is held until new auth present, and then resolves on Server.', () async {
          setAuth();
          var authValue;
          var authCallbackCalledCount = 0;
          var testCompleter = Completer();
          var receivedCallbackInvoked = 0;

          AuthorizationRequiredEvent authCallbackCalledEvent;
          matsSocket.setAuthorizationExpiredCallback((event) {
              authCallbackCalledCount++;
              authCallbackCalledEvent = event;
              Timer(Duration(milliseconds: 50), () {
                  var expiry = DateTime.now().add(Duration(milliseconds: 20000));
                  authValue = 'DummyAuth:MatsSocket.renewAuth_${id(10)}:${expiry.millisecondsSinceEpoch}';
                  matsSocket.setCurrentAuthorization(authValue, expiry, Duration.zero);
              });
          });

          matsSocket.terminator('Client.renewAuth_terminator').listen((messageEvent) {
              // Assert that the Authorization Value is the one we set just above.
              expect(messageEvent.data, equals(authValue));
              // Assert that we got receivedCallback ONCE
              expect(receivedCallbackInvoked, equals(1), reason: 'Should have gotten one, and only one, receivedCallback.');
              // Assert that we got AuthorizationExpiredEventType.REAUTHENTICATE, and only one call to Auth.
              expect(authCallbackCalledEvent.type, equals(AuthorizationRequiredEventType.REAUTHENTICATE), reason: 'Should have gotten AuthorizationRequiredEventType.REAUTHENTICATE authorizationExpiredCallback.');
              expect(authCallbackCalledCount, equals(1), reason: 'authorizationExpiredCallback should only have been invoked once');
              testCompleter.complete();
          });

          var req = {
              'string': 'test',
              'number': math.e,
              'sleepTime': 0
          };
          await matsSocket.send('Test.renewAuth', "MatsSocket.renewAuth_${id(6)}", req);
          receivedCallbackInvoked++;

          await testCompleter.future;
        });
      });

      group('PreConnectionOperation - Authorization upon WebSocket HTTP Handshake', () {
        test('When preconnectoperations=true, we should get the initial AuthorizationValue presented in Cookie in the authPlugin.checkHandshake(..) function Server-side', () async {
          // This is what we're going to test. Cannot be done in Node.js, as there is no common Cookie-jar there.
          matsSocket.preconnectoperation = matsSocket.platform.sendAuthorizationHeader;

          var expiry = DateTime.now().add(Duration(milliseconds: 20000));
          var authValue = 'DummyAuth:PreConnectOperation:${expiry.millisecondsSinceEpoch}';
          matsSocket.setCurrentAuthorization(authValue, expiry, Duration(milliseconds: 5000));

          var value = await matsSocket.request('Test.replyWithCookieAuthorization', 'PreConnectionOperation_${id(6)}', {});
          expect(value.data['string'], equals(authValue));
        });

        test('When the test-servers PreConnectOperation HTTP Auth-to-Cookie Servlet repeatedly returns [400 <= status <= 599], we should eventually get SessionClosedEvent.VIOLATED_POLICY.', () async {
          // This is what we're going to test. Cannot be done in Node.js, as there is no common Cookie-jar there.
          matsSocket.preconnectoperation = matsSocket.platform.sendAuthorizationHeader;
          matsSocket.maxConnectionAttempts = 2; // "Magic option" that is just meant for integration testing.
          var testCompleter = Completer();

          matsSocket.setAuthorizationExpiredCallback((event) {
            var expiry = DateTime.now().add(Duration(milliseconds: 1000));
            var authValue = 'DummyAuth:fail_preConnectOperationServlet:${expiry.millisecondsSinceEpoch}';
            matsSocket.setCurrentAuthorization(authValue, expiry, Duration(milliseconds: 200));
          });

          matsSocket.addSessionClosedEventListener((event) {
            expect(event.type, equals(MatsSocketCloseCodes.VIOLATED_POLICY));
            expect(event.code, equals(MatsSocketCloseCodes.VIOLATED_POLICY.code));
            expect(event.type.name, equals('VIOLATED_POLICY'));
            expect(event.reason.toLowerCase(), contains('too many consecutive'));
            testCompleter.complete();
          });

          await matsSocket.request(
              'Test.replyWithCookieAuthorization', 'PreConnectionOperation_${id(6)}', {}).catchError((messageEvent) {
            expect(messageEvent.type, equals(MessageEventType.SESSION_CLOSED));
          });
          await testCompleter.future;
        });

        test('When the test-servers authPlugin.checkHandshake(..) repeatedly returns false, we should eventually get SessionClosedEvent.VIOLATED_POLICY.', () async {
          // This is what we're going to test. Cannot be done in Node.js, as there is no common Cookie-jar there.
          matsSocket.preconnectoperation = matsSocket.platform.sendAuthorizationHeader;
          matsSocket.maxConnectionAttempts = 2; // "Magic option" that is just meant for integration testing.
          var testCompleter = Completer();

          matsSocket.addConnectionEventListener((event) {
            _logger.info('Connection attempt: ${event.connectionAttempt}, type: ${event.type}, countDown: ${event.countdownSeconds}');
          });

          matsSocket.setAuthorizationExpiredCallback((event) {
            var expiry = DateTime.now().add(Duration(milliseconds: 1000));
            var authValue = 'DummyAuth:fail_checkHandshake:${expiry.millisecondsSinceEpoch}';
            matsSocket.setCurrentAuthorization(authValue, expiry, Duration(milliseconds: 200));
          });

          matsSocket.addSessionClosedEventListener((event) {
            expect(event.type, equals(MatsSocketCloseCodes.VIOLATED_POLICY));
            expect(event.code, equals(MatsSocketCloseCodes.VIOLATED_POLICY.code));
            expect(event.type.name, equals('VIOLATED_POLICY'));
            expect(event.reason.toLowerCase(), contains('too many consecutive'));
            testCompleter.complete();
          });

          await matsSocket.request(
              'Test.replyWithCookieAuthorization', 'PreConnectionOperation_${id(6)}', {}).catchError((messageEvent) {
            expect(messageEvent.type, equals(MessageEventType.SESSION_CLOSED));
          });

          await testCompleter.future;
        }, timeout: Timeout.factor(10));
      });
    });
  });
}
