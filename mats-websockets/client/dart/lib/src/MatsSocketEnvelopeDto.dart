import 'package:mats_socket/src/MessageType.dart';

class MatsSocketEnvelopeDto {
  MessageType type;
  String clientLibAndVersion;
  String appName;
  String appVersion;

  /// Authorization header
  String authorization;

  String sessionId;

  /// Requested debug info (currently only Client-to-Server)
  int requesterDebug;

  /// target MatsSocketEndpointId: Which MatsSocket Endpoint (server/client) this message is for
  String endpointId;

  String traceId;

  /// ServerMessageId, messageId from Server.
  String serverMessageId;

  /// ClientMessageId, messageId from Client.
  String clientMessageId;

  /// Either multiple cmid or smid - used for ACKs/NACKs and ACK2s.
  List<String> ids;

  /// PingId - "correlationId" for pings. Serialized to x, so as to use little space.
  String pingId;

  /// Timeout, for Client-to-Server Requests.
  Duration timeout;

  /// Description when failure (NACK or others), exception message, multiline, may include
  /// stacktrace if auth.
  String description;

  /// Message. This is a dynamic type deserialized from JSON data in the incoming message.
  /// This can be further processed into strong types via dart converters.
  dynamic message;

  // ::: Debug info

  /// Debug info object - enabled if requested ('[requesterDebug]') and principal is allowed.
  Map<String, dynamic> _debug;

  MatsSocketEnvelopeDto({
    this.type,
    this.clientLibAndVersion,
    this.appName,
    this.appVersion,
    this.authorization,
    this.sessionId,
    this.requesterDebug,
    this.endpointId,
    this.traceId,
    this.serverMessageId,
    this.clientMessageId,
    this.ids,
    this.pingId,
    this.timeout,
    this.description,
    this.message,
  });

  MatsSocketEnvelopeDto.empty();

  MatsSocketEnvelopeDto.fromEncoded(Map<String, dynamic> envelope) {
    type = MessageTypeExtension.fromName(envelope['t']);
    message = envelope['msg'];
    description = envelope['desc'];
    timeout = (envelope['to'] != null) ? Duration(milliseconds: envelope['to'] as int) : null;
    appName = envelope['an'];
    appVersion = envelope['av'];
    traceId = envelope['tid'];
    pingId = envelope['x'];
    endpointId = envelope['eid'];
    sessionId = envelope['sid'];
    authorization = envelope['auth'];
    clientMessageId = envelope['cmid']?.toString();
    serverMessageId = envelope['smid'];
    ids = (envelope['ids'] as List<dynamic>)?.map((id) => id as String)?.toList();

    requesterDebug = envelope['rd'];
    _debug = envelope['debug'];
  }

  /// Did we receive debug details in this envelope.
  bool get receivedDebug => _debug != null;

  /// Convert this Envelope to a Json Map representation, this is used by dart:convert
  /// to encode this object for JSON.
  Map<String, dynamic> toJson() {
    var json = {
      't': type.name,
      'tid': traceId,
      'clv': clientLibAndVersion,
      'to': timeout?.inMilliseconds,
      'msg': message,
      'desc': description,
      'eid': endpointId,
      'cmid': clientMessageId,
      'smid': serverMessageId,
      'ids': ids,
      'x': pingId,
      'an': appName,
      'av': appVersion,
      'auth': authorization,
      'sid': sessionId,
      'rd': requesterDebug
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

  /// If debug data was requested, and we also got debug data back from the server, this will return
  /// the DebugDto containing all available debug information. In cases where we request debug information,
  /// but the server denies us access to them, we will still get out debug options related to data stored
  /// client side.
  DebugDto debug(DateTime clientMessageSent, int requestedDebugOptions, DateTime messageReceived) {
    // ?: Did we request debug options, yet none very provided?
    if (requesterDebug != 0 && _debug == null) {
      // Yes -> We need to return a debug value for client data, while leaving all server information empty.
      return DebugDto(
          clientMessageSent: clientMessageSent,
          messageReceived: messageReceived,
          requestedDebugOptions: requestedDebugOptions);
    }
    // ?: Did we receive debug information, and it was also requested?
    else if (_debug != null && requesterDebug != 0) {
      // Yes -> Construct a DebugDto.
      var debug = _debug;

      // Convert all the loglines present in the debug details, if present.
      List<LogLineDto> logLines;
      if (debug['l'] is List<Map<String, dynamic>>) {
        logLines = (debug['l'] as List<Map<String, dynamic>>).map((line) => LogLineDto(
            timestamp: line['t'],
            system: line['s'],
            host: line['hos'],
            appName: line['an'],
            appVersion: line['av'],
            threadName: line['t'],
            level: line['level'],
            message: line['m'],
            exception: line['x'],
            mdc: line['mdc']));
      }
      return DebugDto(
          clientMessageSent: clientMessageSent,
          messageReceived: messageReceived,
          requestedDebugOptions: requestedDebugOptions,
          resolvedDebugOptions: debug['resd'],
          description: debug['d'],
          clientMessageReceived: _readDateTime('cmrts', debug),
          clientMessageReceivedNodename: debug['cmrnn'],
          matsMessageSent: _readDateTime('mmsts', debug),
          matsMessageReplyReceived: _readDateTime('mmrrts', debug),
          matsMessageReplyReceivedNodename: debug['mmrrnn'],
          serverMessageCreated: _readDateTime('smcts', debug),
          serverMessageCreatedNodename: debug['smcnn'],
          messageSentToClient: _readDateTime('mscts', debug),
          messageSentToClientNodename: debug['mscnn'],
          logLines: logLines);
    } else {
      // E -> No debug information present or requested.
      return null;
    }
  }

  /// Internal helper to safely read a date time from the debug data.
  DateTime _readDateTime(String field, Map<String, dynamic> debug) {
    if (debug[field] == null) {
      return null;
    } else if (debug[field] is int) {
      return DateTime.fromMillisecondsSinceEpoch(debug[field]);
    } else {
      throw ArgumentError.value(debug[field], 'Field $field in $debug is not null or a number, cannot parse');
    }
  }
}

/// Meta-information for the call, availability depends on the allowed debug options for the authenticated user,
/// and which information is requested in client. Notice that Client side and Server side might have wildly differing
/// ideas of what the time is, which means that timestamps comparison between Server and Client must be evaluated
/// with massive interpretation.
class DebugDto {
  final DateTime clientMessageSent;
  final DateTime messageReceived;
  final int requestedDebugOptions;
  final int resolvedDebugOptions;
  final String description;
  final DateTime clientMessageReceived;
  final String clientMessageReceivedNodename;
  final DateTime matsMessageSent;
  final DateTime matsMessageReplyReceived;
  final String matsMessageReplyReceivedNodename;
  final DateTime serverMessageCreated;
  final String serverMessageCreatedNodename;
  final DateTime messageSentToClient;
  final String messageSentToClientNodename;
  final List<LogLineDto> logLines;

  DebugDto({
    this.clientMessageSent,
    this.messageReceived,
    this.requestedDebugOptions,
    this.resolvedDebugOptions,
    this.description,
    this.clientMessageReceived,
    this.clientMessageReceivedNodename,
    this.matsMessageSent,
    this.matsMessageReplyReceived,
    this.matsMessageReplyReceivedNodename,
    this.serverMessageCreated,
    this.serverMessageCreatedNodename,
    this.messageSentToClient,
    this.messageSentToClientNodename,
    this.logLines,
  });
}

class LogLineDto {
  final DateTime timestamp; // TimeStamp
  final String system; // System: "MatsSockets", "Mats", "MS SQL" or similar.
  final String host; // Host OS, e.g. "iOS,v13.2", "Android,vKitKat.4.4", "Chrome,v123:Windows,vXP",
  // "Java,v11.03:Windows,v2019"
  final String appName; // AppName
  final String appVersion; // AppVersion
  final String threadName; // Thread name
  final int level; // 0=TRACE, 1=DEBUG, 2=INFO, 3=WARN, 4=ERROR
  final String message; // Message
  final String exception; // Exception if any, null otherwise.
  final Map<String, String> mdc; // The MDC

  LogLineDto({
    this.timestamp,
    this.system,
    this.host,
    this.appName,
    this.appVersion,
    this.threadName,
    this.level,
    this.message,
    this.exception,
    this.mdc,
  });
}
