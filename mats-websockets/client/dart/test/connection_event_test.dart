import 'package:mats_socket/mats_socket.dart';
import 'package:test/test.dart';

void main() {

  group('ConnectionEvent', () {
    group('.countdownSeconds', () {
      test('On timeout 5 seconds with 1 second elapsed, should return 4.0', () {
        var event = ConnectionEvent(
          ConnectionEventType.CONNECTING,
          null,
          null,
          Duration(seconds: 5),
          Duration(seconds: 1)
        );
        expect(event.countdownSeconds, equals('4.0'));
      });

      test('On timeout 5 seconds with 100 microseconds elapsed, should return 5.0', () {
        var event = ConnectionEvent(
            ConnectionEventType.CONNECTING,
            null,
            null,
            Duration(seconds: 5),
            Duration(microseconds: 100)
        );
        expect(event.countdownSeconds, equals('5.0'));
      });
      
      test('On timeout 5 seconds with 1 millisecond elapsed, should return 5.0', () {
        var event = ConnectionEvent(
            ConnectionEventType.CONNECTING,
            null,
            null,
            Duration(seconds: 5),
            Duration(milliseconds: 1)
        );
        expect(event.countdownSeconds, equals('5.0'));
      });

      test('On timeout 5 seconds with 100 millisecond elapsed, should return 4.9', () {
        var event = ConnectionEvent(
            ConnectionEventType.CONNECTING,
            null,
            null,
            Duration(seconds: 5),
            Duration(milliseconds: 100)
        );
        expect(event.countdownSeconds, equals('4.9'));
      });

      test('On timeout 5 seconds with 151 millisecond elapsed, should return 4.8', () {
        var event = ConnectionEvent(
            ConnectionEventType.CONNECTING,
            null,
            null,
            Duration(seconds: 5),
            Duration(milliseconds: 151)
        );
        expect(event.countdownSeconds, equals('4.8'));
      });

    });
  });
}
