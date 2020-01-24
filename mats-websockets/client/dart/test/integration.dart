import 'package:mats_socket/mats_socket.dart';
import 'package:test/test.dart';
import 'package:test_api/src/backend/invoker.dart';
import 'package:web_socket_channel/web_socket_channel.dart';
import 'package:web_socket_channel/io.dart';
import 'package:logging/logging.dart';
import 'dart:math';
import 'dart:io';
import 'logging.dart';

class IOSocketFactory extends WebSocketChannelFactory {
  Future<WebSocketChannel> connect(
      String url, String protocol, String authorization) async {
    return IOWebSocketChannel.connect(url,
        protocols: [protocol],
        headers: {'Authorization': 'Bearer $authorization'},
        pingInterval: Duration(seconds: 10));
  }
}

void setAuth(MatsSocket matsSocket,
    {Duration duration = const Duration(minutes: 1),
    Duration roomForLatencyMillis = const Duration(seconds: 10)}) {
  var now = DateTime.now();
  var expiry = now.add(duration);
  matsSocket.setCurrentAuthorization(
      'DummyAuth:${expiry.millisecondsSinceEpoch}', expiry,
      roomForLatency: roomForLatencyMillis);
}

final log = Logger('Test');

void main() {
  configureLogging();

  final log = Logger('Test');
  DateTime testStart;
  var urls = Platform.environment['MATS_SOCKET_URLS']?.split(",") ??
      [
        'ws://localhost:8080/matssocket',
        'ws://localhost:8081/matssocket'
      ];
  var matsSocket = MatsSocket('Test', '1.0', urls, IOSocketFactory());
  setUp(() {
    testStart = DateTime.now();
    log.info('=== Test [${Invoker.current.liveTest.test.name}] starting');
  });
  tearDown(() async {
    await matsSocket.close('testDone');
    log.info(
        '=== Test [${Invoker.current.liveTest.test.name}] done after [${DateTime.now().difference(testStart)}]');
  });

  group('Authorization', () {
    test('Should invoke authorization callback before making calls', () async {
      var authCallbackCalled = false;
      matsSocket.authorizationCallback = (event) {
        authCallbackCalled = true;
        setAuth(matsSocket);
      };

      await matsSocket.send('Test.single', 'SEND_' + randomId(6), {});
      expect(authCallbackCalled, true);
    });

    test('Should not invoke authorization callback if authorization present',
        () async {
      var authCallbackCalled = false;
      setAuth(matsSocket);
      matsSocket.authorizationCallback = (event) {
        authCallbackCalled = true;
      };

      await matsSocket.send('Test.single', 'SEND_' + randomId(6), {});
      expect(authCallbackCalled, false);
    });

    test('Should invoke authorization callback when expired', () async {
      var authCallbackCalled = false;

      setAuth(matsSocket, duration: Duration(minutes: -10));
      matsSocket.authorizationCallback = (event) {
        authCallbackCalled = true;
        setAuth(matsSocket);
      };

      await matsSocket.send('Test.single', 'SEND_' + randomId(6), {});
      expect(authCallbackCalled, true);
    });

    test('Should invoke authorization callback when room for latency expired',
        () async {
      var authCallbackCalled = false;

      setAuth(matsSocket, roomForLatencyMillis: Duration(minutes: 10));
      matsSocket.authorizationCallback = (event) {
        authCallbackCalled = true;
        setAuth(matsSocket);
      };

      await matsSocket.send('Test.single', 'SEND_' + randomId(6), {});
      expect(authCallbackCalled, true);
    });
  });

  group('send', () {
    test('Should fire and forget', () {
      var matsSocket = authenticatedMatsSocket();
      matsSocket.send('Test.single', 'SEND_${randomId(6)}', {});
    });

    test('Should have a promise that resolves when received', () async {
      var matsSocket = authenticatedMatsSocket();
      var received =
          await matsSocket.send('Test.single', 'SEND_${randomId(6)}', {});
    });
  });

  group('request', () {
    test('Should receive a reply', () async {
      var matsSocket = authenticatedMatsSocket();
      var reply = await matsSocket.request(
          'Test.single',
          'REQUEST-with-Promise_${randomId(6)}',
          {'string': 'Request String', 'number': e});
      expect(reply.traceId, isNotNull);
    });

    test('Should invoke the ack callback', () async {
      var matsSocket = authenticatedMatsSocket();
      var receiveCallbackCalled = false;
      await matsSocket.request(
          'Test.single',
          'REQUEST-with-Promise_${randomId(6)}',
          {'string': 'Request String', 'number': e}, (received) {
        receiveCallbackCalled = true;
      });
      expect(receiveCallbackCalled, equals(true));
    });
  });

  group('requestReplyTo', () {
    test('Should receive a reply on our own endpoint', () async {
      var matsSocket = authenticatedMatsSocket();

      var endpoint = matsSocket.endpoint('ClientSide.testEndpoint');

      await matsSocket.requestReplyTo(
          'Test.single',
          'REQUEST-with-Promise_${randomId(6)}',
          {'string': 'Request String', 'number': e},
          'ClientSide.testEndpoint');
      var reply = await endpoint.first;
      expect(reply.traceId, isNotNull);
    });
  });
}

MatsSocket authenticatedMatsSocket() {
  var urls = Platform.environment['MATS_SOCKET_URLS']?.split(",") ??
      [
        'ws://localhost:8080/matssocket',
        'ws://localhost:8081/matssocket'
      ];
  var matsSocket = MatsSocket('Test', '1.0', urls, IOSocketFactory());

  setAuth(matsSocket);
  return matsSocket;
}
