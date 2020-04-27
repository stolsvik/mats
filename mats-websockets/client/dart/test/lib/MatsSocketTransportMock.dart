
import 'dart:async';
import 'dart:convert';

import 'package:mats_socket/src/MatsSocketEnvelopeDto.dart';
import 'package:mats_socket/src/MatsSocketPlatform.dart';
import 'package:mats_socket/src/MessageType.dart';

MatsSocketPlatform createTransport() => MatsSocketTransportMock.noop();

class MatsSocketTransportMock extends MatsSocketPlatform {

  Function(dynamic) beforeunloadHandler;
  final List<String> closedSessions = [];
  final MessageHandler websocketMessageHandler;

  MatsSocketTransportMock(this.websocketMessageHandler);

  MatsSocketTransportMock.noop(): this((envelope, sink) {
    switch (envelope.type) {
      case MessageType.HELLO:
        {
          sink([
            MatsSocketEnvelopeDto(
                type: MessageType.WELCOME,
                sessionId: '123')
          ]);
        }
        break;
      case MessageType.SEND:
        {
          sink([
            MatsSocketEnvelopeDto(
                type: MessageType.ACK,
                clientMessageId: envelope.clientMessageId)
          ]);
        }
        break;
      case MessageType.REQUEST:
        {}
        break;
      default:
        {}
        break;
    }
  });

  @override
  Future<bool> closeSession(Uri closeUri, String sessionId) {
    closedSessions.add(sessionId);
    return Future.value(true);
  }

  @override
  WebSocket connect(Uri webSocketUri, String protocol, String authorization) {
    return WebSocketMock(webSocketUri.toString(), websocketMessageHandler);
  }

  @override
  void deregisterBeforeunload(Function(dynamic) beforeunloadHandler) {
    this.beforeunloadHandler == null;
  }

  @override
  double performanceTime() {
    return DateTime.now().millisecondsSinceEpoch.toDouble();
  }

  @override
  void registerBeforeunload(Function(dynamic) beforeunloadHandler) {
    this.beforeunloadHandler = beforeunloadHandler;
  }

  @override
  ConnectResult sendAuthorizationHeader(Uri websocketUri, String authorization) {
    return ConnectResult(() {}, Future.value(200));
  }

  @override
  String get userAgent => 'UnitTest userAgent';

}

typedef MessageHandler = Function(MatsSocketEnvelopeDto, Function(Iterable<MatsSocketEnvelopeDto>));

class WebSocketMock extends WebSocket {
  @override
  String url;
  final MessageHandler _messageHandler;

  WebSocketMock(this.url, this._messageHandler) {
    // Open after 10ms
    Timer(Duration(milliseconds: 10), handleOpen);
  }

  WebSocketMock.noop(String url): this(url, (envelope, sink) {
    switch (envelope.type) {
      case MessageType.HELLO:
        {
          sink([
            MatsSocketEnvelopeDto(
                type: MessageType.WELCOME,
                sessionId: '123')
          ]);
        }
        break;
      case MessageType.SEND:
        {
          sink([
            MatsSocketEnvelopeDto(
                type: MessageType.ACK,
                clientMessageId: envelope.clientMessageId)
          ]);
        }
        break;
      case MessageType.REQUEST:
        {}
        break;
      default:
        {}
        break;
    }
  });

  @override
  void close(int code, String reason) {
    handleClose(code, reason);
  }

  @override
  void send(String data) {
    // Dispatch the data as json to the message handler.
    for (var envelopeDto in (jsonDecode(data) as List<dynamic>)) {
      Future(() => _messageHandler(MatsSocketEnvelopeDto.fromEncoded(envelopeDto as Map<String, dynamic>), (items) {
        // The result from the message handler we encode to json, then pass to the handleMessage
        // asynchronously, which will then forward the json to the MatsSocket onMessage handler.
        var responseJson = jsonEncode(items.toList());
        Future(() {
          handleMessage(responseJson);
        });
      }));
    }
  }
}