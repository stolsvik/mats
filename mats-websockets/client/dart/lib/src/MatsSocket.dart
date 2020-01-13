import 'dart:async';
import 'dart:math';
import 'dart:io';
import 'dart:convert';

import 'package:web_socket_channel/web_socket_channel.dart';
import 'package:logging/logging.dart';

DateTime EPOCH = DateTime.fromMicrosecondsSinceEpoch(0);
const ALPHABET =
    '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
const CLIENT_LIB_NAME_AND_VERSION = 'MatsSocket.dart,v0.8.9';
final VERSION =
    '${CLIENT_LIB_NAME_AND_VERSION}; User-Agent: Dart ${Platform.version}';

typedef AuthorizationCallback = Function(AuthorizationRefreshEvent);

/// Generate a random id of the given length
///
String randomId([int length]) {
  var rnd = Random();
  var result = '';
  for (var i = 0; i < length ?? 10; i++) {
    result += ALPHABET[rnd.nextInt(ALPHABET.length)];
  }
  return result;
}

/// Checks if you are awesome. Spoiler: you are.
class MatsSocket {
  String appName;
  String appVersion;
  List<String> wsUrls;
  WebSocketChannelFactory socketFactory;

  // :: Authorization and session variables
  String sessionId;
  AuthorizationCallback _authorizationCallback;
  Completer<Authorization> _authorizationCompleter;
  Authorization _authorization;

  // :: Message pipeline variables
  int pipelineMaxSize = 25;
  Duration maxEnvelopeAge = Duration(milliseconds: 50);
  Duration pipelineDebounceTime = Duration(milliseconds: 10);

  final List<Envelope> _prePipeline = [];
  final List<Envelope> _pipeline = [];
  int _messageSequenceId = 0;
  final Map<int, OutstandingSendOrRequest> _outstandingSendsAndRequests = {};
  Timer _pipelineDebounce;
  final Map<String, StreamController<Envelope>> _endpoints = {};

  // :: WebSocket variables
  int _nextUrlIndex = 0;
  WebSocketChannel _websocketChannel;
  StreamSubscription<Envelope> _socketSubscription;
  Completer _websocketChannelDone;

  final Logger _log = Logger('MatsSocket');

  MatsSocket(this.appName, this.appVersion, this.wsUrls, this.socketFactory) {
    assert(wsUrls.isNotEmpty);
  }

  // ===== Authorization handling ==================================================================

  Future<Authorization> get authorization async {
    // ?: Are we currently waiting for authorization?
    if (_authorizationCompleter?.isCompleted == false) {
      // Yes -> Wait for the future frpm the completer
      _log.info(
          'Authorization completer submitted, but not yet done, returning its future');
      return _authorizationCompleter.future;
    }
    // E -> We are not waiting for an authorization

    // ?: Is there no authorization present?
    if (_authorization == null) {
      // Yes -> First authentication, create a completer to listen for the authorization, and
      //        return its future
      _log.info(
          'No authorization present, informing callback and waiting for a new authorization');
      _authorizationCompleter = Completer<Authorization>();
      _authorizationCallback(AuthorizationRefreshNewEvent());

      // When the authorization is set, the future will resolve from the completer
      await _authorizationCompleter.future;
    }
    // E -> We do have an authorization already

    // Keep looping until we have an authorization that has not expired. If the callback sets an
    // authorization that has expired, this will just loop and wait again for a non-expired
    // authorization.
    while (_authorization.expired) {
      // Yes -> We need to refresh the authorization
      _log.info('Current authorization has expired, requesting a refresh');
      _authorizationCompleter = Completer<Authorization>();
      _authorizationCallback(
          AuthorizationRefreshRefreshEvent(_authorization.expire));

      await _authorizationCompleter.future;
    }
    // --- At this point, we have a non-expired authorization
    _log.fine('Current authorization still valid.');
    return _authorization;
  }

  void setAuthorizationExpiredCallback(
      AuthorizationCallback authorizationCallback) {
    _authorizationCallback = authorizationCallback;
  }

  void setCurrentAuthorization(String authorization, DateTime expire,
      {Duration roomForLatency = const Duration(seconds: 10)}) {
    _authorization =
        Authorization(authorization, expire, roomForLatency: roomForLatency);
    _log.info(
        'Current authorization set to ${_authorization.token}, will expire in ${expire.difference(DateTime.now())}');
    // ?: Do we have an incomplete _authorizationCompleter?
    if (_authorizationCompleter?.isCompleted == false) {
      // Yes -> Set the value, so all futures listening will be notified.
      _log.info(
          'Authorization completer was waiting for an authorization, setting it to complete');
      _authorizationCompleter.complete(_authorization);
    }
  }

  // ===== WebSocketChannel handling ===============================================================

  Future<WebSocketChannel> get websocketChannel async {
    // ?: Is the current channel null, or has a close code set, indicating that it's closed?
    if (_websocketChannel == null || _websocketChannel.closeCode != null) {
      // Yes -> Need to create a new _websocketChannel

      // ?: Do we have a current subscription?
      if (_socketSubscription != null) {
        // Yes -> canel it before we start a new one
        await _socketSubscription.cancel();
      }

      // Wait for the authorization to be present before we connect to the websocket.
      // This to avoid timeouts on the websocket connection, as we have a small window to send
      // HELLO after establishing the connection before we get a timeout.
      var authorization = await this.authorization;

      // Connect to the next url, and increment the counter for next attempt
      var url = wsUrls[_nextUrlIndex];
      _log.info(
          'WebSocket not connected, connecting to url ${_nextUrlIndex} -> ${url}');
      _websocketChannel = await socketFactory.connect(url, 'matssocket');
      _websocketChannelDone = Completer();
      _nextUrlIndex = (_nextUrlIndex + 1) % wsUrls.length;

      // Read the websocket stream. We expect to get json arrays of messages, so we take each
      // payload, decode the json, and process each envelope in isolation.
      var envelopeStream = _websocketChannel.stream.expand((payload) {
        var envelopes = jsonDecode(payload) as List<dynamic>;
        _log.info(
            'Received ${envelopes.length} envelope(s) from payload $payload');
        return envelopes.map((envelope) => Envelope.fromEncoded(envelope));
      });

      _socketSubscription = envelopeStream.listen(_onMessage,
          onError: _onWebSocketError, onDone: _onWebSocketDone);

      // Put the hello envelope into the pre-pipeline
      _log.fine(
          'Adding hello to pre-pipeline, authorization: ${authorization.token}');
      _prePipeline.add(
          Envelope.hello(appName, appVersion, sessionId, authorization.token));
      _pipelineSend(_websocketChannel, false);
      return _websocketChannel;
    } else {
      // current channel is valid
      return _websocketChannel;
    }
  }

  void _onMessage(Envelope envelope) {
    _log.fine('Handling envelope: ${jsonEncode(envelope)}');
    if (envelope.type == EnvelopeType.WELCOME) {
      _log.info(
          'Received welcome from [${envelope.replyMessageToClientNodename}] '
          'after [${envelope.clientMessageCreated.difference(DateTime.now())}], '
          'with sessionId: [${envelope.sessionId}]');
      sessionId = envelope.sessionId;
      return;
    }
    var outstandingSendsAndRequest =
        _outstandingSendsAndRequests[envelope.messageSequenceId];
    var handled = false;

    // ?: Did we have an outstanding receive waiting for the messageSequenceId
    if (outstandingSendsAndRequest?.receive != null &&
        envelope.type == EnvelopeType.RECEIVED) {
      // Yes -> resolve the receive envelope
      _log.info(
          'Message received for sequenceId: [${envelope.messageSequenceId}]');
      outstandingSendsAndRequest._receive.complete(envelope);
      handled = true;
    }

    // ?: Did we have an outstanding reply waiting for the messageSequenceId
    if (outstandingSendsAndRequest?.reply != null &&
        envelope.type == EnvelopeType.REPLY) {
      // Yes -> resolve the reply envelope
      _log.info(
          'Message reply for sequenceId: [${envelope.messageSequenceId}], '
          'receive accepted: [${outstandingSendsAndRequest._receive.isCompleted}]');
      outstandingSendsAndRequest._reply.complete(envelope);
      handled = true;
    }

    // ?: Are we done handling this outstanding request?
    if (outstandingSendsAndRequest?.done == true) {
      // Yes -> delete it so we don't do double delivery
      _outstandingSendsAndRequests.remove(envelope.messageSequenceId);
      _log.fine(
          'Completed handling for outstanding message sequence: [${envelope.messageSequenceId}]');
    }

    // Is this a reply, that is directed to one of our own endpointIds?
    if (envelope.type == EnvelopeType.REPLY &&
        _endpoints.containsKey(envelope.endpointId)) {
      // Yes -> add the message to the endpoint stream.
      _log.info('Recived message for ${envelope.endpointId}');
      _endpoints[envelope.endpointId].add(envelope);
      handled = true;
    }
    // :: Assert that the envelope was handled
    if (!handled) {
      _log.severe('Could not find a handler for message ${envelope.type}');
    }
  }

  void _onWebSocketError(dynamic error, StackTrace stackTrace) async {
    _log.severe('Error in stream, disconnecting: $error, $stackTrace', error,
        stackTrace);
    await _websocketChannel.sink.close();
    _onWebSocketDone();
  }

  void _onWebSocketDone() async {
    _log.info(
        'WebSocket stream is done, closeCode: ${_websocketChannel.closeCode}, '
        'closeReason: ${_websocketChannel.closeReason}');
    await _socketSubscription.cancel();
    // Complete the done future, for anyone waiting.
    _websocketChannelDone.complete('WebsocketDone');
  }

  // ===== Pipeline handling =======================================================================

  Future<Envelope> _addToPipeline(Envelope envelope,
      {Function(Envelope) receiveCallback, bool immediate}) async {
    envelope.messageSequenceId = _messageSequenceId++;

    // ?: Is the session being closed?
    if (envelope.type == EnvelopeType.CLOSE_SESSION) {
      // Yes -> Send now, we don't expect a reply from the server
      _prePipeline.add(envelope);
      _pipeline.clear();
      sessionId = null;
      await _pipelineScheduleSend(immediate: true, closeAfterSend: true);
      return Envelope.empty();
    }

    // If it is a REQUEST /without/ a specific Reply (client) Endpoint defined, then it is a ...
    var requestWithPromiseCompletion = (envelope.type == EnvelopeType.REQUEST &&
        envelope.replyEndpointId == null);
    OutstandingSendOrRequest outstandingSendOrRequest;

    // ?: Is it an ordinary REQUEST with Promise-completion?
    if (requestWithPromiseCompletion) {
      // -> Yes, ordinary REQUEST with Promise-completion
      outstandingSendOrRequest = OutstandingSendOrRequest.request(envelope);
    } else {
      // -> No, it is SEND or REQUEST-with-ReplyTo.
      outstandingSendOrRequest = OutstandingSendOrRequest.send(envelope);
    }

    _pipeline.add(envelope);
    _outstandingSendsAndRequests[envelope.messageSequenceId] =
        outstandingSendOrRequest;
    await _pipelineScheduleSend(immediate: immediate);

    // ?: Are we waiting for a reply?
    if (outstandingSendOrRequest.reply != null) {
      // Yes -> We need to return the reply future from the outstandingSendOrRequest

      // ?: Do we have a callback for the receive message?
      if (receiveCallback != null) {
        // Yes -> handle the callback first, logging if it fails
        try {
          receiveCallback(await outstandingSendOrRequest.receive);
        } on Error catch (e) {
          _log.severe('Error on invoking receive callback [${receiveCallback}]',
              e, e.stackTrace);
        }
      }
      return outstandingSendOrRequest.reply;
    } else {
      return outstandingSendOrRequest.receive;
    }
  }

  Future<void> _pipelineScheduleSend(
      {bool immediate, bool closeAfterSend}) async {
    // ?: Are both pipelines empty?
    if (_prePipeline.isEmpty && _pipeline.isEmpty) {
      // Yes -> Do nothing, nothing to send
      _log.fine('Pipeline is empty, not scheduling send');
      return;
    }

    // ?: Is there a timer waiting to send messages
    if (_pipelineDebounce?.isActive == true) {
      // Yes -> cancel, we will reschedule if needed
      _log.fine('Cancelling current timer to send pipeline messages.');
      _pipelineDebounce.cancel();
    }

    // ?: Are we requested to send immediately?
    if (immediate == true) {
      // Yes -> send immediately
      _log.fine('Immediate send requested, sending now');
      await _pipelineSend(await websocketChannel, closeAfterSend == true);
      return;
    }

    // ?: Is there messages in the pre pipeline?
    if (_prePipeline.isNotEmpty) {
      // Yes -> send immediately
      _log.fine('Pre-pipeline has ${_prePipeline.length}, sending now');
      await _pipelineSend(await websocketChannel, closeAfterSend == true);
      return;
    }

    // ?: Is the pipeline full?
    if (_pipeline.length >= pipelineMaxSize) {
      // Yes -> send immediately
      _log.fine('Pipeline has ${_pipeline.length} messages, sending now');
      await _pipelineSend(await websocketChannel, closeAfterSend == true);
      return;
    }

    // ?: Is the first message in the pipeline getting old?
    var oldestEnvelopeEpochMsTime = _pipeline
        .map((envelope) => envelope.clientMessageCreated.millisecondsSinceEpoch)
        .reduce(min);

    var oldestEnvelopeAge = Duration(
        milliseconds:
            DateTime.now().millisecondsSinceEpoch - oldestEnvelopeEpochMsTime);
    if (oldestEnvelopeAge > maxEnvelopeAge) {
      // Yes -> We should not wait any longer to send
      _log.fine(
          'Oldest envelope has been in pipeline for [${oldestEnvelopeAge}], sending now');
      await _pipelineSend(await websocketChannel, closeAfterSend == true);
      return;
    }

    _log.fine('Scheduling pipeline send in [${pipelineDebounceTime}]');
    _pipelineDebounce = Timer(pipelineDebounceTime, () async {
      await _pipelineSend(await websocketChannel, closeAfterSend == true);
    });
  }

  void _pipelineSend(WebSocketChannel channel, bool closeAfterSend) async {
    if (_prePipeline.isNotEmpty) {
      var message = json.encode(_prePipeline);
      _log.info(
          'Flushing [${_prePipeline.length}] messages from pre-pipeline: ${message}');
      _prePipeline.clear();
      channel.sink.add(message);
    }
    if (_pipeline.isNotEmpty) {
      var message = json.encode(_pipeline);
      _log.info(
          'Flushing [${_pipeline.length}] messages from pipeline: ${message}');
      _pipeline.clear();
      channel.sink.add(message);
    }
    if (closeAfterSend) {
      _log.info('Requested to close after send, closing websocket channel');
      var close = await Future.any([
        channel.sink.close(1, 'Close requested'),
        _websocketChannelDone.future
      ]);
      _log.info('Closed websocket channel ${close}');
    }
  }

  // ===== API methods  ============================================================================

  Future<Envelope> send(String endpointId, String traceId, dynamic message) {
    return _addToPipeline(Envelope.send(endpointId, traceId, message));
  }

  Future<Envelope> request(String endpointId, String traceId, dynamic message,
      [Function(Envelope) receiveCallback]) {
    return _addToPipeline(Envelope.request(endpointId, traceId, message),
        receiveCallback: receiveCallback);
  }

  Future<Envelope> requestReplyTo(String endpointId, String traceId,
      dynamic message, String replyToEndpointId,
      [String correlationId]) {
    return _addToPipeline(Envelope.request(
        endpointId, traceId, message, replyToEndpointId, correlationId));
  }

  Stream<Envelope> endpoint(String endpointId) {
    // :: Assert for double-registrations
    if (_endpoints[endpointId] != null) {
      throw DuplicateEndpointRegistration(
          'Cannot register more than one endpoint to same '
          'endpointId [$endpointId], current: [${_endpoints[endpointId]}');
    }
    _endpoints[endpointId] = StreamController<Envelope>();
    return _endpoints[endpointId].stream;
  }

  Future closeSession(String reason) async {
    await _addToPipeline(Envelope.closeSession(reason));
    return _websocketChannelDone.future;
  }
}

// ======== API contracts ==========================================================================

/// A SocketFactory is used to create new WebSocketChannel for the MatsSocket.
///
/// It should return a new instance of a WebSocketChannel for the current platform.
abstract class WebSocketChannelFactory {
  /// Connect to a WebSocketChannel
  Future<WebSocketChannel> connect(String url, String protocol);
}

// ======== Exceptions =============================================================================

/// Exception for when there is a duplicate registration of the same EndpointId.
class DuplicateEndpointRegistration implements Exception {
  final String message;

  const DuplicateEndpointRegistration(this.message);

  @override
  String toString() => '$runtimeType: $message';
}

// ======== Authorization internal classes =========================================================

abstract class AuthorizationRefreshEvent {}

class AuthorizationRefreshNewEvent extends AuthorizationRefreshEvent {}

class AuthorizationRefreshRefreshEvent extends AuthorizationRefreshEvent {
  DateTime expireTime;

  AuthorizationRefreshRefreshEvent(this.expireTime);
}

class Authorization {
  String token;
  DateTime expire;
  Duration roomForLatency;

  Authorization(this.token, this.expire,
      {this.roomForLatency = const Duration(seconds: 10)});

  bool get expired {
    return DateTime.now().add(roomForLatency).isAfter(expire);
  }
}

// ======== Envelope DTO ===========================================================================

enum EnvelopeType {
  HELLO,
  WELCOME,
  SEND,
  REQUEST,
  RECEIVED,
  REPLY,
  CLOSE_SESSION
}

extension EnvelopeTypeExtension on EnvelopeType {
  String get name {
    var EnvelopeTypeNameLength = runtimeType.toString().length;
    return toString().substring(EnvelopeTypeNameLength + 1);
  }
}

class Envelope {
  EnvelopeType type;
  String subType;
  dynamic data;

  String appName;
  String appVersion;

  String traceId;
  String correlationId;
  String endpointId;
  String replyEndpointId;
  String sessionId;
  String authorization;
  int messageSequenceId;
  DateTime clientMessageCreated = DateTime.now();
  DateTime clientMessageReceived;
  String clientMessageReceivedNodename;
  DateTime matsMessageSent;
  DateTime matsMessageReplyReceived;
  String matsMessageReplyReceivedNodename;
  DateTime replyMessageToClient;
  String replyMessageToClientNodename;

  DateTime messageReceivedOnClient;

  Envelope(
      {this.type,
      this.subType,
      this.data,
      this.appName,
      this.appVersion,
      this.traceId,
      this.correlationId,
      this.endpointId,
      this.replyEndpointId,
      this.sessionId,
      this.authorization,
      this.messageSequenceId,
      this.clientMessageCreated,
      this.clientMessageReceived,
      this.clientMessageReceivedNodename,
      this.matsMessageSent,
      this.matsMessageReplyReceived,
      this.matsMessageReplyReceivedNodename,
      this.replyMessageToClientNodename,
      this.replyMessageToClient,
      this.messageReceivedOnClient});

  Envelope.empty();

  Envelope.fromEncoded(Map<String, dynamic> envelope) {
    type = EnvelopeType.values.firstWhere((t) => t.name == envelope['t']);
    data = envelope['msg'];
    subType = envelope['st'];
    appName = envelope['an'];
    appVersion = envelope['av'];
    traceId = envelope['tid'];
    correlationId = envelope['cid'];
    endpointId = envelope['eid'];
    replyEndpointId = envelope['eeid'];
    sessionId = envelope['sid'];
    authorization = envelope['auth'];
    messageSequenceId =
        (envelope['cmseq'] is String) ? int.parse(envelope['cmseq']) : envelope['cmseq'];

    // Timestamps and handling nodenames (pretty much debug information).
    clientMessageCreated = readDateTime(envelope['cmcts']) ?? DateTime.now();
    clientMessageReceived = readDateTime(envelope['cmrts']);
    clientMessageReceivedNodename = envelope['cmrnn'];
    messageReceivedOnClient = DateTime.now();
    matsMessageSent = readDateTime(envelope['mmsts']);
    matsMessageReplyReceived = readDateTime(envelope['mmrrts']);
    matsMessageReplyReceivedNodename = envelope['mmrrnn'];
    replyMessageToClient = readDateTime(envelope['mscts']);
    replyMessageToClientNodename = envelope['mscnn'];
  }

  Envelope.hello(
      this.appName, this.appVersion, this.sessionId, this.authorization) {
    type = EnvelopeType.HELLO;
    subType = (sessionId == null) ? 'NEW' : 'EXPECT_EXISTING';
  }

  Envelope.send(this.endpointId, this.traceId, this.data) {
    type = EnvelopeType.SEND;
  }

  Envelope.request(this.endpointId, this.traceId, this.data,
      [this.replyEndpointId, this.correlationId]) {
    type = EnvelopeType.REQUEST;
  }

  Envelope.closeSession(String reason) {
    type = EnvelopeType.CLOSE_SESSION;
    traceId = 'MatsSocket_shutdown[$reason]${randomId(6)}';
  }

  /// Convert this Envelope to a Json Map representation, this is used by dart:convert
  /// to encode this object for JSON.
  Map<String, dynamic> toJson() {
    var json = {
      't': type.name,
      'st': subType,
      'tid': traceId,
      'clv': VERSION,
      'ts': DateTime.now().millisecondsSinceEpoch,
      'msg': data,
      'eid': endpointId,
      'reid': replyEndpointId,
      'cmseq': messageSequenceId,
      'cid': correlationId,
      'an': appName,
      'av': appVersion,
      'auth': authorization,
      'sid': sessionId,
      'cmcts': clientMessageCreated?.millisecondsSinceEpoch,
      'cmrts': clientMessageReceived?.millisecondsSinceEpoch,
      'cmrnn': clientMessageReceivedNodename,
      'mmsts': matsMessageSent?.millisecondsSinceEpoch,
      'mmrrts': matsMessageReplyReceived?.millisecondsSinceEpoch,
      'mmrrnn': matsMessageReplyReceivedNodename,
      'mscts': replyMessageToClient?.millisecondsSinceEpoch,
      'mscnn': replyMessageToClientNodename
    };

    // Need to remove all keys where we have a null value, otherwise those fields will be
    // included in the serialized json
    var nullKeys = [];
    json.forEach((key, value) {
      if (value == null) {
        nullKeys.add(key);
      }
    });
    nullKeys.forEach(json.remove);

    return json;
  }

  DateTime readDateTime(dynamic millisecondsSinceEpoch) {
    if (millisecondsSinceEpoch == null) {
      return null;
    } else {
      return DateTime.fromMillisecondsSinceEpoch(millisecondsSinceEpoch as int);
    }
  }
}

// ======== Internal state =========================================================================

class OutstandingSendOrRequest {
  Envelope envelope;
  final Completer<Envelope> _receive = Completer();
  Completer<Envelope> _reply;

  OutstandingSendOrRequest.send(this.envelope);

  OutstandingSendOrRequest.request(this.envelope) {
    _reply = Completer();
  }

  // If reply is null (because this is not a request), then isComplete will be
  // null, which is not equal to false. So we resolve to only checking receive
  // if reply is null.
  bool get done => _receive.isCompleted && _reply?.isCompleted != false;

  Future<Envelope> get receive => _receive.future;

  Future<Envelope> get reply => _reply?.future;
}
