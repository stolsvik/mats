import 'dart:async';
import 'dart:math';
import 'dart:io';
import 'dart:convert';
import 'package:http/http.dart' as http;
import 'package:mats_socket/mats_socket.dart';

import 'package:web_socket_channel/web_socket_channel.dart';
import 'package:logging/logging.dart';

DateTime EPOCH = DateTime.fromMicrosecondsSinceEpoch(0);
const ALPHABET =
    '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
const CLIENT_LIB_NAME_AND_VERSION = 'MatsSocket.dart,v0.10.0';
// TODO: Include Flutter SDK if relevant - use dynamic invocation. -est.
final VERSION =
    '${CLIENT_LIB_NAME_AND_VERSION}; Dart ${Platform.version}';

final Logger _log = Logger('MatsSocket');

typedef AuthorizationCallback = Function(AuthorizationRefreshEvent);
/// Callback to handle close session out of band, will receive the websocket url, and the sessionId
typedef OutOfBandClose = Function(String, String);

void defaultOutOfBandClose(String url, String sessionId) {
  // Fire off a "close" over HTTP, so that we avoid dangling sessions
  var closeSesionUrl = url.replaceAll('ws', 'http') + '/close_session?session_id=' + sessionId;
  _log.info("  \\- Send an out-of-band (i.e. HTTP) close_session to '$closeSesionUrl'.");
  // Fire and forget, we do not need to get a reply from the server
  http.post(closeSesionUrl);
}

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

// https://stackoverflow.com/a/12646864/39334
void shuffleList(List items) {
  var rnd = Random();
  for (var i = items.length - 1; i > 0; i--) {
    var j = rnd.nextInt(i + 1);
    var temp = items[i];
    items[i] = items[j];
    items[j] = temp;
  }
}

class MatsSocket {
  final String appName;
  final String appVersion;
  final WebSocketChannelFactory socketFactory;

  // :: Callbacks
  AuthorizationCallback authorizationCallback = (event) {
    throw AuthorizationCallbackMissingException('Missing AuthorizationCallback to handle $event');
  };
  OutOfBandClose outOfBandClose = defaultOutOfBandClose;

  // :: Authorization and session variables
  String sessionId;
  Completer<String> _authorizationCompleter;
  Authorization _authorization;

  // :: Message pipeline configuration
  int pipelineMaxSize = 25;
  Duration maxEnvelopeAge = Duration(milliseconds: 20);
  Duration pipelineDebounceTime = Duration(milliseconds: 2);

  // :: Message pipeline state
  final List<Envelope> _pipeline = [];
  int _messageSequenceId = 0;
  final Map<int, OutstandingSendOrRequest> _outstandingSendsAndRequests = {};
  Timer _pipelineDebounce;
  final Map<String, StreamController<Envelope>> _endpoints = {};

  // :: WebSocket variables
  final List<String> _wsUrls = [];
  int _urlIndex = 0;
  WebSocketChannel _websocketChannel;
  StreamSubscription<Envelope> _socketSubscription;
  Completer _welcomeReceived;
  Completer _websocketChannelDone;


  MatsSocket(this.appName, this.appVersion, List<String> wsUrls, this.socketFactory) {
    assert(wsUrls.isNotEmpty);
    _wsUrls.addAll(wsUrls);
    shuffleList(_wsUrls);
  }

  // ===== API methods  ============================================================================

  /// Change the wsUrls that the MatsSocket connects to.
  ///
  /// This will force the current connection, if open, to close, putting it in a fresh state. This
  /// is because we will need to create a new session against the new urls, as well as start with
  /// our internal state fresh.
  void setWsUrls(List<String> wsUrls) async {
    assert(wsUrls.isNotEmpty);
    // Close the existing connection, if open
    await close('Changing wsUrls');
    // Update the list of urls to use
    _wsUrls.clear();
    _wsUrls.addAll(wsUrls);
    shuffleList(_wsUrls);
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
      _authorizationCompleter.complete(authorization);
    }
  }

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

  Future close(String reason) async {
    var existingSessionId = sessionId;
    var isOpen = _websocketChannel != null && _websocketChannel.closeCode == null;
    var currentWsUrl = _wsUrls[_urlIndex];
    _log.info('close(): Closing MatsSocketSession, id:[$existingSessionId] due to [$reason], '
        'currently connected: [${(isOpen ? currentWsUrl : "not connected")}]');

    // :: In-band session close
    // ?: Do we have WebSocket?
    if (isOpen) {
      // -> Yes, so close WebSocket with MatsSocket-specific CloseCode 4000.
      _log.info(' \\-> WebSocket is open, so we close it with MatsSocket-specific CloseCode CLOSE_SESSION (4000).');

      // We don't want the onDone callback invoked from this event that we initiated ourselves.
      _socketSubscription.onDone(null);
      // We don't want any messages either, as we'll now be clearing out futures and outstanding messages ("acks")
      _socketSubscription.onData(null);
      // Also drop onError for good measure.
      _socketSubscription.onError(null);
      // Cancel the subscription fully before we close the channel
      await _socketSubscription.cancel();
      // Perform the close
      await _websocketChannel.sink.close(4000, reason);
    } else {
      _log.info(' \\-> WebSocket NOT open, so CANNOT close it with MatsSocket-specific CloseCode CLOSE_SESSION (4000).');
    }

    // :: Clear out the state of this MatsSocket.
    _websocketChannel = null;
    sessionId = null;
    _urlIndex = 0;
    _clearPipelineAndOutstandingMessages('session close');

    // :: Out-of-band session close
    // ?: Do we have a sessionId?
    if (existingSessionId != null) {
      // Yes -> Invoke the OutOfBandClose callback
      outOfBandClose(currentWsUrl, existingSessionId);
    }
  }

  // ===== Authorization handling ==================================================================

  Future<String> get _authorizationToken async {
    // ?: Are we currently waiting for authorization?
    if (_authorizationCompleter?.isCompleted == false) {
      // Yes -> Wait for the future from the completer
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
      _authorizationCompleter = Completer<String>();
      authorizationCallback(AuthorizationRefreshNewEvent());

      // When the authorization is set, the future will resolve from the completer
      await _authorizationCompleter.future;
    }
    // E -> We do have an authorization already

    // Keep looping until we have an authorization that has not expired. If the callback sets an
    // authorization that has expired, this will just loop and wait again for a non-expired
    // authorization.
    if (_authorization.expired) {
      // Yes -> We need to refresh the authorization
      _log.info('Current authorization has expired, requesting a refresh');
      _authorizationCompleter = Completer<String>();
      authorizationCallback(
          AuthorizationRefreshRefreshEvent(_authorization.expire));

      await _authorizationCompleter.future;
    }
    // --- At this point, we have a non-expired authorization
    _log.fine('Current authorization still valid.');
    return _authorization.token;
  }

  // ===== WebSocketChannel handling ===============================================================

  Future<WebSocketChannel> get _channel async {
    // ?: Is the current channel null, or has a close code set, indicating that it's closed?
    if (_websocketChannel == null || _websocketChannel.closeCode != null) {
      // Yes -> Need to create a new _websocketChannel

      // ?: Do we have a current subscription?
      if (_socketSubscription != null) {
        // Yes -> canel it before we start a new one
        await _socketSubscription.cancel();
      }
      var attemptRound = 0; // When cycled one time through URLs, increases.
      var timeoutBase = 500; // Milliseconds for this fallback level. Doubles, up to max defined below.
      var timeoutMax = 10000; // Milliseconds max between connection attempts.

      var urlIndexTryingToConnect = 0;

      _log.info('Attempting to connect, candidate urls: [$_wsUrls]');
      // Infinite loop until we connect
      while (true) {
        // Wait for the authorization to be present before we connect to the websocket.
        // This to avoid timeouts on the websocket connection, as we have a small window to send
        // HELLO after establishing the connection before we get a timeout.
        // Also, if the connection times out as we do an incremental backoff, we might end up in a
        // situation where the token has expired, so we need to ensure a fresh token for each attempt

        // Based on whether there is multiple URLs, or just a single one, we choose the short "timeout base", or a longer one (max/2), as minimum.
        var minTimeout = _wsUrls.length > 1 ? timeoutBase : timeoutMax / 2;
        // Timeout: LESSER of "max" and "timeoutBase * (2^round)", which should lead to timeoutBase x1, x2, x4, x8 - but capped at max.
        // .. but at least 'minTimeout'
        var attemptTimeout = min(timeoutMax, timeoutBase * pow(2, attemptRound));
        var timeout = Duration(milliseconds: max(minTimeout, attemptTimeout).toInt());

        var authorizationToken = await _authorizationToken;
        var url = _wsUrls[urlIndexTryingToConnect];
        _log.info(' \\- Create WebSocket: attempt [$attemptRound], trying to connect to [$url] within [$timeout]');
        try {
          var socketFuture = socketFactory.connect(url, 'matssocket', authorizationToken);
          _websocketChannel = await socketFuture.then((channel) async {
            _welcomeReceived = Completer();
            _websocketChannelDone = Completer();

            // Read the websocket stream. We expect to get json arrays of messages, so we take each
            // payload, decode the json, and process each envelope in isolation.
            var envelopeStream = channel.stream.expand((payload) {
              var envelopes = jsonDecode(payload) as List<dynamic>;
              _log.info(
                  'Received ${envelopes.length} envelope(s) from payload $payload');
              return envelopes.map((envelope) => Envelope.fromEncoded(envelope));
            });

            // Set the handler for the subscription
            _socketSubscription = envelopeStream.listen(_onMessage);
            // If the subscription results in an error, for example that we are not able to connect,
            // or we can't upgrade to a WebSocket channel, the error should be forwarded to the
            // welcome received completer, so that this loop fails exceptionally, and retries.
            _socketSubscription.onError(_welcomeReceived.completeError);
            // If we are disconnected before we receive welcome, we also need to abort
            _socketSubscription.onDone(() { _welcomeReceived.completeError(Envelope.error()); });

            // Put the hello envelope into the pre-pipeline
            _log.fine('Adding hello to pre-pipeline, authorization: $authorizationToken');
            var prePipeline = [Envelope.hello(appName, appVersion, sessionId, authorizationToken)];
            await _pipelineSend(channel, prePipeline);

            // Wait for us to receive the welcome message successfully. If this fails, it means
            // that we where not able to connect, and need to retry. This is handled in the catch
            // case that waits for the websocket channel.
            await _welcomeReceived.future;

            // We are welcome, change the error handlers to the default, as well as set up the
            // done handler.
            _socketSubscription.onError(_onWebSocketError);
            _socketSubscription.onDone(_onWebSocketDone);
            return channel;
          }).timeout(timeout);

          _urlIndex = urlIndexTryingToConnect;
          _log.info(' \\- Create WebSocket: success in connecting to $url');
          return _websocketChannel;
        } on TimeoutException {
          // :: Timeout handling, retry
          _log.info(' \\- Create WebSocket: Timeout exceeded [$timeout], this WebSocket is bad so ditch it.');
          attemptRound += 1;
          urlIndexTryingToConnect = (urlIndexTryingToConnect + 1) % _wsUrls.length;
          await _socketSubscription?.cancel();
        } catch(e) {
          // :: Any connection failure handling
          _log.info(' \\- Create WebSocket: Failed to connect to [$url].', e);
          attemptRound += 1;
          urlIndexTryingToConnect = (urlIndexTryingToConnect + 1) % _wsUrls.length;
          await _socketSubscription?.cancel();
        }
      } // -- Connection while loop
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
      _welcomeReceived.complete();
      return;
    }
    var outstandingSendsAndRequest =
        _outstandingSendsAndRequests[envelope.messageSequenceId];
    var handled = false;

    // ?: Did we have an outstanding receive waiting for the messageSequenceId
    if (outstandingSendsAndRequest?.receive != null &&
        envelope.type == EnvelopeType.RECEIVED) {
      // Yes -> resolve the receive envelope
      if (envelope.subType == EnvelopeSubType.ACK) {
        _log.info(
            'Message ACK received for sequenceId: [${envelope.messageSequenceId}]');
        outstandingSendsAndRequest._receive.complete(envelope);
      } else {
        outstandingSendsAndRequest._receive.completeError(envelope);
      }
      handled = true;
    }

    // ?: Did we have an outstanding reply waiting for the messageSequenceId
    if (outstandingSendsAndRequest?.reply != null &&
        envelope.type == EnvelopeType.REPLY) {
      // Yes -> resolve the reply envelope
      _log.info('Recived [${enumName(envelope.subType)}] '
          'for sequenceId: [${envelope.messageSequenceId}]');

      // ?: Is this a resolve type, or an error
      if (envelope.subType == EnvelopeSubType.RESOLVE) {
        outstandingSendsAndRequest._reply.complete(envelope);
      } else {
        outstandingSendsAndRequest._reply.completeError(envelope);
      }
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
      _log.info('Recived [${enumName(envelope.subType)}] message for ${envelope.endpointId}');

      // ?: Is this a resolve type, or an error
      if (envelope.subType == EnvelopeSubType.RESOLVE) {
        _endpoints[envelope.endpointId].add(envelope);
      } else {
        _endpoints[envelope.endpointId].addError(envelope);
      }
      handled = true;
    }
    // :: Assert that the envelope was handled
    if (!handled) {
      _log.severe('Could not find a handler for message '
          '[${enumName(envelope.type)}/${enumName(envelope.subType)}]');
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
      {bool immediate}) async {
    // ?: Are both pipelines empty?
    if (_pipeline.isEmpty) {
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
      await _pipelineSend(await _channel);
      return;
    }

    // ?: Is the pipeline full?
    if (_pipeline.length >= pipelineMaxSize) {
      // Yes -> send immediately
      _log.fine('Pipeline has ${_pipeline.length} messages, sending now');
      await _pipelineSend(await _channel);
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
      await _pipelineSend(await _channel);
      return;
    }

    _log.fine('Scheduling pipeline send in [${pipelineDebounceTime}]');
    _pipelineDebounce = Timer(pipelineDebounceTime, () async {
      await _pipelineSend(await _channel);
    });
  }

  void _pipelineSend(WebSocketChannel channel, [List<Envelope> prePipeline]) async {
    if (prePipeline != null && prePipeline.isNotEmpty) {
      var message = json.encode(prePipeline);
      _log.info(
          'Flushing [${prePipeline.length}] messages from pre-pipeline: ${message}');
      prePipeline.clear();
      channel.sink.add(message);
    }
    if (_pipeline.isNotEmpty) {
      // Wait until we are welcome until we send further messages, this is to avoid clearing
      // the pipeline until we have a valid connection.
      await _welcomeReceived.future;

      var message = json.encode(_pipeline);
      _log.info(
          'Flushing [${_pipeline.length}] messages from pipeline: ${message}');
      _pipeline.clear();
      channel.sink.add(message);
    }
  }

  void _clearPipelineAndOutstandingMessages(reason) {
    // :: Clear pipeline
    _pipeline.length = 0;

    // :: Reject all outstanding receives and replies
    _outstandingSendsAndRequests.forEach((cmid, outstanding) {
      if (!outstanding._receive.isCompleted) {
        _log.info('Clearing outstanding receive on [$cmid] to '
            '[${outstanding.envelope.endpointId}].');
        outstanding._receive.completeError(
            MatsSocketCloseException(reason), StackTrace.current);
      }
      if (outstanding?._reply?.isCompleted == false) {
        _log.info('Clearing outstanding reply on [$cmid] to '
            '[${outstanding.envelope.endpointId}].');
        outstanding._reply.completeError(
            MatsSocketCloseException(reason), StackTrace.current);
      }
    });
  }

}

// ======== API contracts ==========================================================================

/// A SocketFactory is used to create new WebSocketChannel for the MatsSocket.
///
/// It should return a new instance of a WebSocketChannel for the current platform.
abstract class WebSocketChannelFactory {
  /// Connect to a WebSocketChannel
  Future<WebSocketChannel> connect(String url, String protocol, String authorization);
}

// ======== Exceptions =============================================================================

/// Exception for when there is a duplicate registration of the same EndpointId.
class DuplicateEndpointRegistration implements Exception {
  final String message;

  const DuplicateEndpointRegistration(this.message);

  @override
  String toString() => '$runtimeType: $message';
}

class MatsSocketCloseException implements Exception {
  final String message;

  const MatsSocketCloseException(this.message);

  @override
  String toString() => '$runtimeType: $message';
}

class AuthorizationCallbackMissingException implements Exception {
  final String message;

  const AuthorizationCallbackMissingException(this.message);

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
  CLOSE_SESSION,
  ERROR
}

enum EnvelopeSubType {
  NEW, RECONNECT, ACK, NACK, ERROR, RESOLVE, REJECT
}

String enumName(dynamic value) {
  if (value == null) {
    return null;
  }
  var typeNameLength = value.runtimeType.toString().length;
  return value.toString().substring(typeNameLength + 1);
}

EnvelopeType envelopeType(String value) {
  if (value == null) {
    return null;
  }
  var filter = (e) => enumName(e) == value;
  return EnvelopeType.values.firstWhere(filter, orElse: () => throw Exception('No EnvelopeType for $value'));
}

EnvelopeSubType envelopeSubType(String value) {
  if (value == null) {
    return null;
  }
  var filter = (e) => enumName(e) == value;
  return EnvelopeSubType.values.firstWhere(filter, orElse: () => throw Exception('No EnvelopeSubType for $value'));
}

class Envelope {
  EnvelopeType type;
  EnvelopeSubType subType;
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
    type = envelopeType(envelope['t']);
    data = envelope['msg'];
    subType = envelopeSubType(envelope['st']);
    appName = envelope['an'];
    appVersion = envelope['av'];
    traceId = envelope['tid'];
    correlationId = envelope['cid'];
    endpointId = envelope['eid'];
    replyEndpointId = envelope['eeid'];
    sessionId = envelope['sid'];
    authorization = envelope['auth'];
    messageSequenceId =
        (envelope['cmid'] is String) ? int.parse(envelope['cmid']) : envelope['cmid'];

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
    subType = (sessionId == null) ? EnvelopeSubType.NEW : EnvelopeSubType.RECONNECT;
  }

  Envelope.send(this.endpointId, this.traceId, this.data) {
    type = EnvelopeType.SEND;
  }

  Envelope.request(this.endpointId, this.traceId, this.data,
      [this.replyEndpointId, this.correlationId]) {
    type = EnvelopeType.REQUEST;
  }

  Envelope.error() {
    type = EnvelopeType.ERROR;
    subType = EnvelopeSubType.ERROR;
  }

  /// Convert this Envelope to a Json Map representation, this is used by dart:convert
  /// to encode this object for JSON.
  Map<String, dynamic> toJson() {
    var json = {
      't': enumName(type),
      'st': enumName(subType),
      'tid': traceId,
      'clv': VERSION,
      'ts': DateTime.now().millisecondsSinceEpoch,
      'msg': data,
      'eid': endpointId,
      'reid': replyEndpointId,
      'cmid': messageSequenceId,
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

  bool get done {
    // ?: Do we have a reply?
    if (_reply != null) {
      // Yes -> We are done when we have the reply envelope
      return _reply.isCompleted;
    } else {
      // No -> We are done when we have the receive envelope
      return _receive.isCompleted;
    }
  }

  Future<Envelope> get receive {
    // ?: Do we have a reply?
    if (_reply != null) {
      // Yes -> Wait for either receive or reply
      return Future.any([_receive.future, _reply.future]);
    } else {
      // Yes -> Wait for receive
      return _receive.future;
    }
  }

  Future<Envelope> get reply => _reply?.future;
}
