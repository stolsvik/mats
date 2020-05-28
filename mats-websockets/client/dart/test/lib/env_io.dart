import 'package:logging/logging.dart';
import 'dart:io';

import 'package:mats_socket/src/ConnectionEvent.dart';

List<Uri> loadServerUris() {
  var envUrls = Platform.environment['MATS_SOCKET_URLS'] ??
      'ws://localhost:8080/matssocket,ws://localhost:8081/matssocket';
  return envUrls.split(',').map((url) => Uri.parse(url)).toList();

}

int code(ConnectionEvent connectionEvent) {
  return (connectionEvent.webSocketEvent as Map<String, dynamic>)['code'] as int;
}

String reason(ConnectionEvent connectionEvent) {
  return (connectionEvent.webSocketEvent as Map<String, dynamic>)['reason'] as String;
}

/// Helper class to configure dart logging to print to stdout.
Level logLevel() {
  // We can set the log level through the environment variables, which enables
  // setting the level from gradle.
  var envLogLevel = Platform.environment['LOG_LEVEL'] ?? 'INFO';
  switch (envLogLevel) {
    case 'DEBUG': return Level.ALL;
    case 'INFO': return Level.INFO;
    default: return Level.SEVERE;
  }
}