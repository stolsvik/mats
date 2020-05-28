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

Level logLevel() {
  return Level.INFO;
}