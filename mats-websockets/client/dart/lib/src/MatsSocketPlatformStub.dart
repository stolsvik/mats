import 'MatsSocketPlatform.dart';

/// Stub implementation when there both dart:io and dart:html is missing.
MatsSocketPlatform createTransport() => throw UnsupportedError('Cannot create a MatsSocketTransport without dart:html or dart:io.');