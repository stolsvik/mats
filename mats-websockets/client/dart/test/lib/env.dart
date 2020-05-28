import 'package:mats_socket/mats_socket.dart';
import 'package:mats_socket/src/ConnectionEvent.dart';
import 'package:logging/logging.dart';

import 'package:test_api/src/backend/invoker.dart' show Invoker;
import 'env_html.dart' if (dart.library.io) 'env_io.dart' as delegate;

var logContext = 'none';

/// Helper class to configure dart logging to print to stdout.
void configureLogging() {
  Logger.root.level = delegate.logLevel();

  print('Setting log level to ${Logger.root.level}');

  Logger.root.onRecord.listen((LogRecord rec) {
    print('${rec.time.toIso8601String()} ${rec.level.name} ${rec.loggerName.padRight(24)} |$logContext| ${rec.message}');
    if (rec.error != null) {
      print('\tError: ${rec.error}');
    }
    if (rec.stackTrace != null) {
      print(rec.stackTrace);
    }
  });
}

MatsSocket createMatsSocket() {
  var matsSocket = MatsSocket('TestApp', '1.2.3', delegate.loadServerUris());
  logContext = matsSocket.matsSocketInstanceId;
  // Print rather than log the mats socket creation, so that the start of each test
  // is marked clearly in the logs
  print('=========== Created MatsSocket [${matsSocket.matsSocketInstanceId}] '
      'for test [${Invoker?.current?.liveTest?.test?.name}] ===========');
  return matsSocket;
}

var serverUris = delegate.loadServerUris();

int code(ConnectionEvent connectionEvent) {
  return delegate.code(connectionEvent);
}

String reason(ConnectionEvent connectionEvent) {
  return delegate.reason(connectionEvent);
}