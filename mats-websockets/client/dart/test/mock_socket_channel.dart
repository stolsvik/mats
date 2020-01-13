import 'dart:async';
import 'dart:convert';

import 'package:mats_socket/mats_socket.dart';
import 'package:web_socket_channel/web_socket_channel.dart';
import 'package:mockito/mockito.dart';

typedef MessageHandler = Function(Envelope, Function(Iterable<Envelope>));

class MockWebSocketSink implements WebSocketSink {

  final streamController = StreamController();
  int closeCode;
  String closeReason;

  @override
  Future get done => streamController.done;

  @override
  Future addStream(Stream stream) => stream.forEach(add);

  @override
  void add(var data) => streamController.add(data);

  @override
  Future close([int closeCode, String closeReason]) {
    this.closeCode = closeCode;
    this.closeReason = closeReason;
    return streamController.close();
  }

  @override
  void addError(error, [StackTrace stackTrace]) =>
      streamController.addError(error, stackTrace);
}

class MockWebSocketChannel extends Mock implements WebSocketChannel {

  String url;
  String _protocol;
  String _closeReason;
  int _closeCode;
  MockWebSocketSink mockWebSocketSink = MockWebSocketSink();

  final streamController = StreamController();

  @override
  Stream get stream => streamController.stream;

  @override
  WebSocketSink get sink => mockWebSocketSink;

  @override
  String get protocol => _protocol;

  @override
  int get closeCode => _closeCode;

  @override
  String get closeReason => _closeReason;

  MockWebSocketChannel(this.url, this._protocol);

}

class MockSocketFactory extends WebSocketChannelFactory {

  final MessageHandler _messageHandler;

  MockSocketFactory(this._messageHandler);

  MockSocketFactory.noop() : this((envelope, sink) {
    switch (envelope.type) {
      case EnvelopeType.HELLO : {
        sink([Envelope(
          type: EnvelopeType.WELCOME,
          subType: 'NEW',
          sessionId: '123'
        )]);
      }
      break;
      case EnvelopeType.SEND: {
        sink([Envelope(
          type: EnvelopeType.RECEIVED,
          messageSequenceId: envelope.messageSequenceId
        )]);
      }
      break;
      case EnvelopeType.REQUEST: {}
      break;
      case EnvelopeType.CLOSE_SESSION: {}
      break;
      default: {}
        break;
    }
  });

  Future<WebSocketChannel> connect(String url, String protocol) async {
    print('Connecting');
    var channel = MockWebSocketChannel(url, protocol);
    channel.mockWebSocketSink.streamController.stream.expand((payload) {
      var envelopes = jsonDecode(payload) as List<dynamic>;
      return envelopes.map((envelope) => Envelope.fromEncoded(envelope));
    }).listen((envelope) {
      _messageHandler(envelope, (replies) {
        channel.streamController.add(jsonEncode(replies));
      });
    });
    return channel;
  }
}

