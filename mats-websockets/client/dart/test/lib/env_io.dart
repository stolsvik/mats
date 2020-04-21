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
void configureLogging() {
  // We can set the log level through the environment variables, which enables
  // setting the level from gradle.
  var envLogLevel = Platform.environment['LOG_LEVEL'] ?? 'INFO';
  switch (envLogLevel) {
    case 'DEBUG': { Logger.root.level = Level.ALL; }
    break;
    case 'INFO': { Logger.root.level = Level.INFO; }
    break;
    default: { Logger.root.level = Level.SEVERE; }
    break;
  }

  print('Setting log level to $envLogLevel');

  Logger.root.onRecord.listen((LogRecord rec) {
    print('${rec.time} ${rec.level.name} ${rec.loggerName.padRight(12)} | ${rec.message}');
    if (rec.error != null) {
      print('\tError: ${rec.error}');
    }
    if (rec.stackTrace != null) {
      print(rec.stackTrace);
    }
  });
}