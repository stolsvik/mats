import 'dart:async';

import 'package:mats_socket/mats_socket.dart';
import 'package:test/test.dart';
import 'lib/env.dart';
import 'dart:math' as math;

void main() {
  configureLogging();

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

    setUp(() {
      matsSocket = MatsSocket('TestApp', '1.2.3', serverUris);
    });

    tearDown(() async  {
      await matsSocket.close('Test done');
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
        Future testIt(String userId, bool reauthBeforeRecieve) async {
          setAuth(userId, Duration(seconds: 2), Duration.zero);

          var authEventCompleter = Completer<AuthorizationRequiredEventType>.sync();
          var receivedEventCompleter = Completer<ReceivedEvent>.sync();
          var replyCompleter = Completer<MessageEvent>.sync();

          matsSocket.setAuthorizationExpiredCallback((event) {
            authEventCompleter.complete(event.type);
            // This standard auth does not fail reevaluateAuthentication.
            setAuth();
          });
          var req = {'string': 'test', 'number': math.e, 'sleepTime': 0};
          // Ignore the future returned, we will instead wait for the reply completer
          var _ = matsSocket.request('Test.slow', 'REQUEST_authentication_from_server_${id(6)}', req,
              ackCallback: receivedEventCompleter.complete,
              nackCallback: receivedEventCompleter.completeError,
              timeout: Duration(seconds: 15))
              .then(replyCompleter.complete, onError: replyCompleter.completeError);

          // ?: Do we expect to reauth before receive?
          if (reauthBeforeRecieve) {
            // 1. Wait for the auth callback to complete
            var authCallbackCalledEventType = await authEventCompleter.future.timeout(Duration(seconds: 1));
            // Assert that we got AuthorizationExpiredEventType.REAUTHENTICATE, and only one call to Auth.
            expect(authCallbackCalledEventType, equals(AuthorizationRequiredEventType.REAUTHENTICATE),
                reason: 'Should have gotten AuthorizationRequiredEventType.REAUTHENTICATE authorizationExpiredCallback.');

            // We expext to reauth before we get the ack, so receivedEventCompleter should not be complete yet.
            expect(receivedEventCompleter.isCompleted, isFalse, reason: 'AuthCallback should be invoked before ReceivedEvent');

            // 2. Get the receive event from the server
            await receivedEventCompleter.future;
            // At this point, the auth callback should not have been called, nor the reply
            expect(replyCompleter.isCompleted, isFalse, reason: 'Reply should not be sent before ReceivedEvent');

          }
          else {
            // 1. Get the receive event from the server
            await receivedEventCompleter.future;
            // At this point, the auth callback should not have been called, nor the reply
            expect(authEventCompleter.isCompleted, isFalse, reason: 'AuthCallback should not be invoked before ReceivedEvent');
            expect(replyCompleter.isCompleted, isFalse, reason: 'Reply should not be sent before ReceivedEvent');

            // 2. Wait for the auth callback to complete
            var authCallbackCalledEventType = await authEventCompleter.future.timeout(Duration(seconds: 1));
            // Assert that we got AuthorizationExpiredEventType.REAUTHENTICATE, and only one call to Auth.
            expect(authCallbackCalledEventType, equals(AuthorizationRequiredEventType.REAUTHENTICATE),
                reason: 'Should have gotten AuthorizationRequiredEventType.REAUTHENTICATE authorizationExpiredCallback.');
          }

          expect(replyCompleter.isCompleted, isFalse, reason: 'Reply should not be sent before Receive and reauth');

          // 3. We now expect the reply to complete, and we can get the data
          var data = (await replyCompleter.future).data;

          // Assert data, with the changes from Server side.
          expect(data['string'], equals('${req['string']}:FromSlow'));
          expect(data['number'], equals(req['number']));
          expect(data['sleepTime'], equals(req['sleepTime']));
        }

        test('Receive: Using special userId which DummyAuthenticator fails on step reevaluateAuthentication(..), Server shall ask for REAUTH when we perform Request, and when gotten, resolve w/o retransmit (server "holds" message).', () async {
          // Using special "userId" for DummySessionAuthenticator that specifically fails @ reevaluateAuthentication(..) step
          await testIt('fail_reevaluateAuthentication', true);
        });

        test('Reply from Server: Using special userId which DummyAuthenticator fails on step reevaluateAuthenticationForOutgoingMessage(..), Server shall require REAUTH from Client before sending Reply.', () async {
          // Using special "userId" for DummySessionAuthenticator that specifically fails @ reevaluateAuthenticationForOutgoingMessage(..) step
          await testIt('fail_reevaluateAuthenticationForOutgoingMessage', false);
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
          matsSocket.maxConsecutiveFailsOrErrors = 2; // "Magic option" that is just meant for integration testing.
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
          matsSocket.maxConsecutiveFailsOrErrors = 2; // "Magic option" that is just meant for integration testing.
          var testCompleter = Completer();

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
