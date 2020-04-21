import 'dart:html';

import 'package:logging/logging.dart';
import 'package:mats_socket/src/ConnectionEvent.dart';

List<Uri> loadServerUris() {
  return [Uri.parse('ws://localhost:8080/matssocket'), Uri.parse('ws://localhost:8081/matssocket')];
}

int code(ConnectionEvent connectionEvent) {
  return (connectionEvent.webSocketEvent as CloseEvent).code;
}

String reason(ConnectionEvent connectionEvent) {
  return (connectionEvent.webSocketEvent as CloseEvent).reason;
}

/// Helper class to configure dart logging to print to stdout.
void configureLogging() {

  print('Setting log level to INFO');
  Logger.root.level = Level.INFO;

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