import 'dart:async';
import 'dart:html' as html;
import 'dart:html';
import 'package:logging/logging.dart';

import 'MatsSocketPlatform.dart';

final Logger _logger = Logger('MatsSocket.transportHtml');

class MatsSocketPlatformHtml extends MatsSocketPlatform {

  String cookies;

  @override
  WebSocket connect(Uri webSocketUri, String protocol, String authorization) {
    _logger.fine('Creating HTML WebSocket to $webSocketUri with protocol: $protocol');
    return HtmlWebSocket.create(webSocketUri.toString(), protocol, authorization);
  }

  @override
  Future<bool> closeSession(Uri closeUri, String sessionId) async {
    return html.window.navigator.sendBeacon(closeUri.toString(), null);
  }

  @override
  String get userAgent => html.window.navigator.userAgent;

  @override
  ConnectResult sendAuthorizationHeader(Uri websocketUri, String authorization) {
    final completer = Completer<int>();
    // Create an XMLHttpRequest
    final xhr = html.HttpRequest();
    xhr.open('GET', websocketUri.toString().replaceAll('ws', 'http'));
    xhr.setRequestHeader('Authorization', authorization);

    // Taken from html.HttpRequest.request in Dart html package.
    xhr.onLoad.listen((e) async {
      _logger.fine('  \\ - Received reply from server $e - status: ${xhr.status}');
      // Get XHR's status
      var status = xhr.status;
      // ?: Was it a GOOD return?
      if ((status == 200) || (status == 202) || (status == 204)) {
        // -> Yes, it was good - supplying the status code
        completer.complete(status);
      } else {
        // -> Not, it was BAD - supplying the status code
        completer.completeError(status);
      }
    });
    xhr.withCredentials = true;
    _logger.fine('Sending authorization headers to $websocketUri using XMLHttpRequest $xhr');
    xhr.send();
    return ConnectResult(xhr.abort, completer.future.timeout(Duration(seconds: 10)));
  }

  @override
  void deregisterBeforeunload(Function(dynamic) beforeunloadHandler) {
    html.window.addEventListener('beforeunload', beforeunloadHandler);
  }

  @override
  void registerBeforeunload(Function(dynamic) beforeunloadHandler) {
    html.window.removeEventListener('beforeunload', beforeunloadHandler);
  }

  @override
  double performanceTime() {
    return html.window.performance.now();
  }

}

class HtmlWebSocket extends WebSocket {
  String _url;
  html.WebSocket _htmlWebSocket;

  HtmlWebSocket.create(String url, String protocol, String authorization) {
    _url = url;
    _htmlWebSocket = html.WebSocket(url, protocol);
    _htmlWebSocket.onClose.forEach((closeEvent) {
      handleClose(closeEvent.code, closeEvent.reason, closeEvent);
    });
    _htmlWebSocket.onError.forEach(handleError);
    _htmlWebSocket.onMessage.forEach((messageEvent) {
      handleMessage(messageEvent.data as String, messageEvent);
    });
    _htmlWebSocket.onOpen.forEach(handleOpen);
  }

  static HtmlWebSocket connect(String url, String protocol, String authorization) {
    return HtmlWebSocket.create(url, protocol, authorization);
  }

  @override
  void close(int code, String reason) {
    _htmlWebSocket.close(code, reason);
  }

  @override
  void send(String data) {
    _htmlWebSocket.send(data);
  }

  @override
  String get url => _url;
}

MatsSocketPlatform createTransport() => MatsSocketPlatformHtml();
