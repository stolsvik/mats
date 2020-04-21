import 'package:test/test.dart';

import 'lib/MatsSocketTransportMock.dart';
import 'lib/env.dart';
import 'package:mats_socket/mats_socket.dart';

void main() {
  MatsSocketTransportMock transportMock;

  configureLogging();

  setUp(() {
    transportMock = MatsSocketTransportMock.noop();
  });

  group('MatsSocket constructor', () {

    test('Should fail on empty url list', () {
      expect(() => MatsSocket('', '', [], transportMock), throwsA(TypeMatcher<AssertionError>()));
    });

    test('Should accept a single wsUrl', () {
      MatsSocket('', '', [Uri.dataFromString('ws://test/')], transportMock);
    });
  });

  group('Authorization', () {
    test('Should invoke authorization callback before making calls', () async {
      var matsSocket = MatsSocket('Test', '1.0', [Uri.dataFromString('ws://localhost:8080/')], transportMock);
      var authCallbackCalled = false;      
      matsSocket.setAuthorizationExpiredCallback((event) {
        authCallbackCalled = true;
        matsSocket.setCurrentAuthorization('Test', DateTime.now().add(Duration(minutes: 1)));
      });

      await matsSocket.send('Test.authCallback', 'SEND_${id(6)}', '');

      expect(authCallbackCalled, true);
    });

    test('Should not invoke authorization callback if authorization present', () async {
      var matsSocket = MatsSocket('Test', '1.0', [Uri.dataFromString('ws://localhost:8080/')], transportMock);
      var authCallbackCalled = false;      
      matsSocket.setCurrentAuthorization('Test', DateTime.now().add(Duration(minutes: 1)));
      matsSocket.setAuthorizationExpiredCallback((event) {
        authCallbackCalled = true;
      });

      await matsSocket.send('Test.authCallback', 'SEND_${id(6)}', '');

      expect(authCallbackCalled, false);
    });

    test('Should invoke authorization callback when expired', () async {
      var matsSocket = MatsSocket('Test', '1.0', [Uri.dataFromString('ws://localhost:8080/')], transportMock);

      var authCallbackCalled = false;

      matsSocket.setCurrentAuthorization('Test', DateTime.now().subtract(Duration(minutes: 1)));
      matsSocket.setAuthorizationExpiredCallback((event) {
        authCallbackCalled = true;
        matsSocket.setCurrentAuthorization('Test', DateTime.now().add(Duration(minutes: 1)));
      });

      await matsSocket.send('Test.authCallback', 'SEND_${id(6)}', '');

      expect(authCallbackCalled, true);
    });

    test('Should invoke authorization callback when room for latency expired', () async {
      var matsSocket = MatsSocket('Test', '1.0', [Uri.dataFromString('ws://localhost:8080/')], transportMock);

      var authCallbackCalled = false;

      matsSocket.setCurrentAuthorization('Test', DateTime.now().subtract(Duration(minutes: 1)), Duration(minutes: 10));
      matsSocket.setAuthorizationExpiredCallback((event) {
        authCallbackCalled = true;
        matsSocket.setCurrentAuthorization('Test', DateTime.now().add(Duration(minutes: 1)));
      });

      await matsSocket.send('Test.authCallback', 'SEND_' + id(6), '');

      expect(authCallbackCalled, true);
    });
  });
}
