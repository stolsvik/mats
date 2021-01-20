import 'package:mats_socket/src/MatsSocketPlatform.dart';
import 'package:test/test.dart';

import 'lib/env.dart';

void main() {
  configureLogging();

  group('MatsSocketTransport', () {
    test('Able to create a new instance', () {
      MatsSocketPlatform.create();
    });

    test('Able to access version', () {
      expect(MatsSocketPlatform.create().version, isNotNull);
    });

    test('Version contains dart version', () {
      expect(MatsSocketPlatform.create().version, contains('; dart:v'));
    });

    test('Only a single ; in the version, seperating OS and Dart version', () {
      // 2 parts, the OS part and the Dart part
      expect(MatsSocketPlatform.create().version.split(RegExp('; ')), hasLength(2));
    });

    test('OS Version starts with a digit', () {
      expect(MatsSocketPlatform.create().version, matches(RegExp(r'^\w+,v\d+.*')));
    });

  });
}