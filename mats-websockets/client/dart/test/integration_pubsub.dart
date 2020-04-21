import 'dart:async';

import 'package:logging/logging.dart';
import 'package:mats_socket/mats_socket.dart';
import 'package:mats_socket/src/InitiationProcessedEvent.dart';
import 'package:test/test.dart';
import 'lib/env.dart';
import 'dart:math' as math;

void main() {
  configureLogging();

  final _logger = Logger('integration_pubsub');

  group('MatsSocket integration tests of "pub/sub" - Publish and Subscribe', () {
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

    group('reconnect', () {
      test('Sub/Pub - preliminary.', () async {
        setAuth();
        var messageEvent = Completer<MessageEvent>();
        matsSocket.subscribe('Test.topic', messageEvent.complete);

        await matsSocket.send('Test.publish', 'PUBLISH_testSend${id(5)}', 'Testmessage');
        await messageEvent.future;
      });
    });
  });
}
