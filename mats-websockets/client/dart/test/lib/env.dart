import 'package:mats_socket/src/ConnectionEvent.dart';

import 'env_html.dart' if (dart.library.io) 'env_io.dart' as delegate;

void configureLogging() => delegate.configureLogging();

var serverUris = delegate.loadServerUris();

int code(ConnectionEvent connectionEvent) {
  return delegate.code(connectionEvent);
}

String reason(ConnectionEvent connectionEvent) {
  return delegate.reason(connectionEvent);
}