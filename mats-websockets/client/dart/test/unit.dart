import 'package:mats_socket/mats_socket.dart';
import 'package:test/test.dart';

import 'logging.dart';
import 'mock_socket_channel.dart';


void main() {
  MockSocketFactory socketFactory;

  configureLogging();

  setUp(() {
    socketFactory = MockSocketFactory.noop();
  });

  group('MatsSocket constructor', () {

    test('Should fail on empty url list', () {
      expect(() => MatsSocket('', '', [], socketFactory), throwsA(TypeMatcher<AssertionError>()));
    });

    test('Should accept a single wsUrl', () {
      MatsSocket('', '', ['ws://test/'], socketFactory);
    });
  });

  group('Authorization', () {
    test('Should invoke authorization callback before making calls', () async {
      var matsSocket = MatsSocket('Test', '1.0', ['ws://localhost:8080/'], socketFactory);
      var authCallbackCalled = false;      
      matsSocket.setAuthorizationExpiredCallback((event) {
        authCallbackCalled = true;
        matsSocket.setCurrentAuthorization('Test', DateTime.now().add(Duration(minutes: 1)));
      });

      await matsSocket.send('Test.authCallback', 'SEND_${randomId(6)}', '');

      expect(authCallbackCalled, true);
    });

    test('Should not invoke authorization callback if authorization present', () async {
      var matsSocket = MatsSocket('Test', '1.0', ['ws://localhost:8080/'], socketFactory);
      var authCallbackCalled = false;      
      matsSocket.setCurrentAuthorization('Test', DateTime.now().add(Duration(minutes: 1)));
      matsSocket.setAuthorizationExpiredCallback((event) {
        authCallbackCalled = true;  
      });

      await matsSocket.send('Test.authCallback', 'SEND_${randomId(6)}', '');

      expect(authCallbackCalled, false);
    });

    test('Should invoke authorization callback when expired', () async {
      var matsSocket = MatsSocket('Test', '1.0', ['ws://localhost:8080/'], socketFactory);

      var authCallbackCalled = false;

      matsSocket.setCurrentAuthorization('Test', DateTime.now().subtract(Duration(minutes: 1)));
      matsSocket.setAuthorizationExpiredCallback((event) {
        authCallbackCalled = true;
        matsSocket.setCurrentAuthorization('Test', DateTime.now().add(Duration(minutes: 1)));
      });

      await matsSocket.send('Test.authCallback', 'SEND_${randomId(6)}', '');

      expect(authCallbackCalled, true);
    });

    test('Should invoke authorization callback when room for latency expired', () async {
      var matsSocket = MatsSocket('Test', '1.0', ['ws://localhost:8080/'], socketFactory);

      var authCallbackCalled = false;

      matsSocket.setCurrentAuthorization('Test', DateTime.now().subtract(Duration(minutes: 1)), roomForLatency: Duration(minutes: 10));
      matsSocket.setAuthorizationExpiredCallback((event) {
        authCallbackCalled = true;
        matsSocket.setCurrentAuthorization('Test', DateTime.now().add(Duration(minutes: 1)));
      });

      await matsSocket.send('Test.authCallback', 'SEND_' + randomId(6), '');

      expect(authCallbackCalled, true);
    });

    test('Should clear authorization when urls are changed', () async {
      var matsSocket = MatsSocket('Test', '1.0', ['ws://localhost:8080/'], socketFactory);

      var authCallbackCalled = false;

      matsSocket.setCurrentAuthorization('Test', DateTime.now().add(Duration(minutes: 1)));

      await matsSocket.send('Test.authCallback', 'SEND_' + randomId(6), '');

      expect(matsSocket.sessionId, isNotNull);

      matsSocket.wsUrls = ['ws://localhost:8081/'];

      expect(matsSocket.sessionId, isNull);
    });
  });
}
