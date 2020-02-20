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
      matsSocket.authorizationCallback = (event) {
        authCallbackCalled = true;
        matsSocket.setCurrentAuthorization('Test', DateTime.now().add(Duration(minutes: 1)));
      };

      await matsSocket.send('Test.authCallback', 'SEND_${randomId(6)}', '');

      expect(authCallbackCalled, true);
    });

    test('Should not invoke authorization callback if authorization present', () async {
      var matsSocket = MatsSocket('Test', '1.0', ['ws://localhost:8080/'], socketFactory);
      var authCallbackCalled = false;
      matsSocket.setCurrentAuthorization('Test', DateTime.now().add(Duration(minutes: 1)));
      matsSocket.authorizationCallback = (event) {
        authCallbackCalled = true;  
      };

      await matsSocket.send('Test.authCallback', 'SEND_${randomId(6)}', '');

      expect(authCallbackCalled, false);
    });

    test('Should invoke authorization callback when expired', () async {
      var matsSocket = MatsSocket('Test', '1.0', ['ws://localhost:8080/'], socketFactory);

      var authCallbackCalled = false;

      matsSocket.setCurrentAuthorization('Test', DateTime.now().subtract(Duration(minutes: 1)));
      matsSocket.authorizationCallback = (event) {
        authCallbackCalled = true;
        matsSocket.setCurrentAuthorization('Test', DateTime.now().add(Duration(minutes: 1)));
      };

      await matsSocket.send('Test.authCallback', 'SEND_${randomId(6)}', '');

      expect(authCallbackCalled, true);
    });

    test('Should invoke authorization callback when room for latency expired', () async {
      var matsSocket = MatsSocket('Test', '1.0', ['ws://localhost:8080/'], socketFactory);

      var authCallbackCalled = false;

      matsSocket.setCurrentAuthorization('Test', DateTime.now().subtract(Duration(minutes: 1)), roomForLatency: Duration(minutes: 10));
      matsSocket.authorizationCallback = (event) {
        authCallbackCalled = true;
        matsSocket.setCurrentAuthorization('Test', DateTime.now().add(Duration(minutes: 1)));
      };

      await matsSocket.send('Test.authCallback', 'SEND_' + randomId(6), '');

      expect(authCallbackCalled, true);
    });
  });

  group('Send/Receive', () {
    test('Fail reply on receive failed', () async {
      var socketFactory = MockSocketFactory.withHello((envelope, sink) {
        sink([
          Envelope(
              type: EnvelopeType.RECEIVED,
              subType: EnvelopeSubType.AUTH_FAIL,
              messageSequenceId: envelope.messageSequenceId)
        ]);
      });
      var matsSocket = MatsSocket('Test', '1.0', ['ws://localhost:8080/'], socketFactory);
      matsSocket.setCurrentAuthorization('', DateTime.now().add(Duration(hours: 1)));

      expect(() async => matsSocket.request('Test.failed', 'send', ''),
          throwsException);
    });

    test('Fail reply on reply failed failed', () async {
      var socketFactory = MockSocketFactory.withHello((envelope, sink) {
        sink([
          Envelope(
              type: EnvelopeType.REPLY,
              subType: EnvelopeSubType.REJECT,
              messageSequenceId: envelope.messageSequenceId)
        ]);
      });
      var matsSocket = MatsSocket('Test', '1.0', ['ws://localhost:8080/'], socketFactory);
      matsSocket.setCurrentAuthorization('', DateTime.now().add(Duration(hours: 1)));

      expect(() async => matsSocket.request('Test.failed', 'send', ''),
          throwsException);
    });

    test('Trigger received on Reply', () async {
      var socketFactory = MockSocketFactory.withHello((envelope, sink) {
        sink([
          Envelope(
              type: EnvelopeType.REPLY,
              subType: EnvelopeSubType.RESOLVE,
              messageSequenceId: envelope.messageSequenceId)
        ]);
      });
      var matsSocket = MatsSocket('Test', '1.0', ['ws://localhost:8080/'], socketFactory);
      matsSocket.setCurrentAuthorization('', DateTime.now().add(Duration(hours: 1)));

      var receiveCalled = false;
      await matsSocket.request('Test.failed', 'send', '', (r) { receiveCalled = true; });

      expect(receiveCalled, equals(true));
    });

    test('Handle reply before received', () async {
      var socketFactory = MockSocketFactory.withHello((envelope, sink) {
        sink([
          Envelope(
              type: EnvelopeType.REPLY,
              subType: EnvelopeSubType.RESOLVE,
              messageSequenceId: envelope.messageSequenceId),
          Envelope(
              type: EnvelopeType.RECEIVED,
              subType: EnvelopeSubType.ACK,
              messageSequenceId: envelope.messageSequenceId)
        ]);
      });
      var matsSocket = MatsSocket('Test', '1.0', ['ws://localhost:8080/'], socketFactory);
      matsSocket.setCurrentAuthorization('', DateTime.now().add(Duration(hours: 1)));

      var receiveCalled = false;
      await matsSocket.request('Test.failed', 'send', '', (r) { receiveCalled = true; });

      expect(receiveCalled, equals(true));
    });
  });
}
