import 'MatsSocketPlatformStub.dart'
    if (dart.library.io) 'MatsSocketPlatformNative.dart'
    if (dart.library.html) 'MatsSocketPlatformHtml.dart';

typedef PreConnectOperation = ConnectResult Function(Uri, String);

/// Interface to communicate with the platform that the mats socket runs on.
///
/// Although Dart is cross platform, unfortunately not all libraries are accessible in both vm and javascript
/// version. To abstract away the parts that MatsSocket needs that do not have a common Dart interface, this
/// class was created.
abstract class MatsSocketPlatform {
  const MatsSocketPlatform();

  /// Create a new instance of the MatsSocketPlatform.
  ///
  /// This method will delegate to appropriate implementation based on the current platform.
  factory MatsSocketPlatform.create() => createTransport();

  /// Send a MatsSocket close session signal.
  ///
  /// When a MatsSocket instance is closing, it also needs to instruct the server so that the sessionId
  /// can be cleaned up. This should be done in a way that is guaranteed to execute before the current
  /// process ends. This means using sendBeacon in a browser context, which queues up the request separate
  /// from the current tab. In DartVM we don't have the same mechanism, as there is no mother process.
  /// Still, the process should not exit until the close request has completed, as we block on the future returned
  /// here.
  Future<bool> closeSession(Uri closeUri, String sessionId);

  WebSocket connect(Uri webSocketUri, String protocol, String authorization);

  String get userAgent;

  ConnectResult sendAuthorizationHeader(Uri websocketUri, String authorization);

  void registerBeforeunload(Function(dynamic) beforeunloadHandler);

  void deregisterBeforeunload(Function(dynamic) beforeunloadHandler);

  double performanceTime();
}

class ConnectResult {
  final Function abortFunction;
  final Future<int> responseStatusCode;

  ConnectResult(this.abortFunction, this.responseStatusCode);

  ConnectResult.noop() : this(() {}, Future.value());
}



/// Handler for the open event, will be passed the native event as an argument, if any.
typedef OpenHandler = void Function(WebSocket, dynamic);

/// Handler for the error event, will be passed the native event as an argument, if any.
typedef ErrorHandler = void Function(WebSocket, dynamic);

/// Handler for messages, this will get the message string, and the native event, if any.
typedef MessageHandler = void Function(WebSocket, String, dynamic);

/// Handler for close event, with WebSocket closeCode and reason, and the native event, if any.
typedef CloseHandler = void Function(WebSocket, int, String, dynamic);

abstract class WebSocket {
  String webSocketInstanceId;

  String get url;

  ErrorHandler _errorHandler;
  MessageHandler _messageHandler;
  CloseHandler _closeHandler;
  OpenHandler _openHandler;

  set onClose(CloseHandler onclose) => _closeHandler = onclose;

  set onError(ErrorHandler errorHandler) => _errorHandler = errorHandler;

  set onOpen(OpenHandler openHandler) => _openHandler = openHandler;

  set onMessage(MessageHandler onclose) => _messageHandler = onclose;

  void handleClose(int code, String reason, [dynamic nativeEvent]) {
    if (_closeHandler != null) {
      _closeHandler(this, code, reason, nativeEvent);
    }
  }

  void handleError([dynamic nativeEvent]) {
    if (_errorHandler != null) {
      _errorHandler(this, nativeEvent);
    }
  }

  void handleOpen([dynamic nativeEvent]) {
    if (_openHandler != null) {
      _openHandler(this, nativeEvent);
    }
  }

  void handleMessage(dynamic message, [dynamic nativeEvent]) {
    if (_messageHandler != null) {
      _messageHandler(this, message as String, nativeEvent);
    }
  }

  void close(int code, String reason);

  void send(String data);
}
