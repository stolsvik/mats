import 'package:logging/logging.dart';
import 'dart:io';

/// Helper class to configure dart logging to print to stdout.
void configureLogging() {
  // We can set the log level through the environment variables, which enables
  // setting the level from gradle.
  switch (Platform.environment['LOG_LEVEL'] ?? 'DEBUG') {
    case 'DEBUG': { Logger.root.level = Level.ALL; }
    break;
    case 'INFO': { Logger.root.level = Level.INFO; }
    break;
    default: { Logger.root.level = Level.SEVERE; }
    break;
  }

  print('Setting log level to ${Platform.environment['LOG_LEVEL'] ?? 'DEBUG'}');

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