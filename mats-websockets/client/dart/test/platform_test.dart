import 'package:mats_socket/src/MatsSocketPlatform.dart';
import 'package:test/test.dart';

import 'lib/env.dart';

void main() {
  configureLogging();

  group('MatsSocketTransport', () {
    test('Able to create a new instance', () {
      MatsSocketPlatform.create();
    });

    test('Able to access user agent', () {
      expect(MatsSocketPlatform.create().userAgent, isNotNull);
    });

  });
}