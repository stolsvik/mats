import 'dart:convert';

import 'package:mats_socket/src/MatsSocketEnvelopeDto.dart';

/// Message Event - the event emitted for a {@link MatsSocket#request() Requests}'s Promise resolve() and reject()
/// (i.e. then() and catch()), and to a {@link MatsSocket#terminator() Terminator}'s resolveCallback and
/// rejectCallback functions for replies due to {@link MatsSocket#requestReplyTo() requestReplyTo}, and for Server
/// initiated Sends (to Terminators), and for the event to a {@link MatsSocket#endpoint() Endpoint} upon a Server
/// initiated Request, and for the event sent to a {@link MatsSocket#subscribe() Subscription}.
class MessageEvent {
  /// Values are from {@link MessageEventType}: Either {@link MessageEventType#SEND "send"} (for a Client
  /// Terminator when targeted for a Server initiated Send); {@link MessageEventType#REQUEST "request"} (for a
  /// Client Endpoint when targeted for a Server initiated Request); or {@link MessageEventType#RESOLVE "resolve"}
  /// or {@link MessageEventType#REJECT "reject"} (for settling of Promise from a Client-initiated Request, and
  /// for a Client Terminator when targeted as the reply-endpoint for a Client initiated Request) - <b>or
  /// {@link MessageEventType#SESSION_CLOSED "sessionclosed"} if the session was closed with outstanding Requests
  /// and MatsSocket therefore "clears out" these Requests.</b>
  /// <p/>
  /// Notice: In the face of {@link MessageType#SESSION_CLOSED "sessionclosed"} or {@link MessageType#TIMEOUT "timeout"},
  /// the {@link #data} property (i.e. the actual message from the server) will be <code>undefined</code>.
  /// Wrt. "sessionclosed", this is <i>by definition</i>: The Request was outstanding, meaning that an answer from the
  /// Server had yet to come. This is opposed to a normal REJECT settling from the Server-side MatsSocketEndpoint,
  /// which may choose to include data with a rejection. The same basically goes wrt. "timeout", as the Server
  /// has not replied yet.
  ///
  final MessageEventType type;

  /// The actual data from the other peer.
  /// <p/>
  /// Notice: In the face of {@link MessageType#SESSION_CLOSED "sessionclosed"} or {@link MessageType#TIMEOUT "timeout"},
  /// this value will be <code>undefined</code>.
  /// Wrt. "sessionclosed", this is <i>by definition</i>: The Request was outstanding, meaning that an answer from the
  /// Server had yet to come. This is opposed to a normal REJECT settling from the Server-side MatsSocketEndpoint,
  /// which may choose to include data with a rejection. The same basically goes wrt. "timeout", as the Server
  /// has not replied yet.
  ///
  final dynamic data;

  /// When a Terminator gets invoked to handle a Reply due to a Client initiated {@link MatsSocket#requestReplyTo},
  /// this holds the 'correlationInformation' object that was supplied in the requestReplyTo(..) invocation.
  ///
  dynamic correlationInformation;

  /// The TraceId for this call / message.
  final String traceId;

  /// Either the ClientMessageId if this message is a Reply to a Client-initiated Request (i.e. this message is a
  /// RESOLVE or REJECT), or ServerMessageId if this originated from the Server (i.e. SEND or REQUEST);
  final String messageId;

  /// millis-since-epoch when the Request, for which this message is a Reply, was sent from the
  /// Client. If this message is not a Reply to a Client-initiated Request, it is undefined.
  DateTime clientRequestTimestamp;

  /// When the message was received on the Client, millis-since-epoch.
  final DateTime receivedTimestamp;

  /// For {@link MatsSocket#request()} and {@link MatsSocket#requestReplyTo()} Requests: Round-trip time in
  /// milliseconds from Request was performed to Reply was received, basically <code>{@link #receivedTimestamp} -
  /// {@link #clientRequestTimestamp}</code>, but depending on the browser/runtime, you might get higher resolution
  /// than integer milliseconds (i.e. fractions of milliseconds, a floating point number) - it depends on the
  /// resolution of <code>performance.now()</code>.
  ///
  /// <b>Note that this number can be a float, not necessarily integer</b>.
  double roundTripMillis;

  /// If debugging is requested, by means of {@link MatsSocket#debug} or the config object in the send, request and
  /// requestReplyTo, this will contain a {@link DebugInformation} instance. However, the contents of that object
  /// is decided by what you request, and what the authorized user is allowed to get as decided by the
  /// AuthenticationPlugin when authenticating the user.
  DebugDto debug;

  MessageEvent(this.type, this.data, this.traceId, this.messageId, this.receivedTimestamp);

  Map<String, dynamic> toJson() {
    return {
      'type': type.name,
      // If data is not json serializable, it will fail to send as well, so
      // we should be ok to include this directly
      'data': data,
      'traceId': traceId,
      'messageId': messageId,
      'receivedTimestamp': receivedTimestamp?.millisecondsSinceEpoch,
      'clientRequestTimestamp': clientRequestTimestamp?.millisecondsSinceEpoch,
      // correlationInformation is never sent, so we have no garuantees that its
      // serializable, so we will have to rely on toString.
      'correlationInformation': correlationInformation?.toString(),
      'debug': debug,
      'roundTripMillis': roundTripMillis
    };
  }
}

/// Types of {@link MessageEvent}.
enum MessageEventType {
  RESOLVE,
  REJECT,
  SEND,
  REQUEST,
  PUB,

  /// "Synthetic" event in that it is not a message from Server: A Client-to-Server
  /// {@link MatsSocket#request() Request} was not replied to by the server within the
  /// {@link MatsSocket#requestTimeoutMillis default request timeout} - or a specific timeout specified in the request
  /// invocation. In these situations, the Request Promise is rejected with a {@link MessageEvent} of this type,
  /// and the {@link MessageEvent#data} value is undefined.
  TIMEOUT,

  /// "Synthetic" event in that it is not a message from Server: This only happens if the MatsSocketSession is
  /// closed with outstanding Client-to-Server {@link MatsSocket#request() Requests} not yet replied to by the
  /// server. In these situations, the Request Promise is rejected with a {@link MessageEvent} of this type, and
  /// the {@link MessageEvent#data} value is undefined.
  SESSION_CLOSED
}

extension MessageEventTypeExtension on MessageEventType {
  String get name {
    switch (this) {
      case MessageEventType.RESOLVE:
        return 'RESOLVE';
      case MessageEventType.REJECT:
        return 'REJECT';
      case MessageEventType.SEND:
        return 'SEND';
      case MessageEventType.REQUEST:
        return 'REQUEST';
      case MessageEventType.PUB:
        return 'PUB';
      case MessageEventType.TIMEOUT:
        return 'TIMEOUT';
      case MessageEventType.SESSION_CLOSED:
        return 'SESSION_CLOSED';
      default:
        throw ArgumentError.value(this, 'MessageEventType', 'Not a recognized enum value');
    }
  }
}

/// <b>Copied directly from AuthenticationPlugin.java</b>:
/// Types of debug information you can request, read more at {@link MatsSocket#debug} and {@link MessageEvent#debug}.
enum DebugOption {
  /// Timing info of the separate phases. Note that time-skew between different nodes must be taken into account.
  TIMESTAMPS,

  /// Node-name of the handling nodes of the separate phases.
  NODES,

  /// <code>AuthenticationPlugin</code>-specific "Option A" - this is not used by MatsSocket itself, but can be employed
  /// and given a meaning by the <code>AuthenticationPlugin</code>.
  /// <p/>
  /// Notice: You might be just as well off by implementing such functionality on the <code>Principal</code> returned by
  /// the <code>AuthenticationPlugin</code> ("this user is allowed to request these things") - and on the request DTOs
  /// from the Client ("I would like to request these things").
  CUSTOM_A,

  /// <code>AuthenticationPlugin</code>-specific "Option B" - this is not used by MatsSocket itself, but can be employed
  /// and given a meaning by the <code>AuthenticationPlugin</code>.
  /// <p/>
  /// Notice: You might be just as well off by implementing such functionality on the <code>Principal</code> returned by
  /// the <code>AuthenticationPlugin</code> ("this user is allowed to request these things") - and on the request DTOs
  /// from the Client ("I would like to request these things").
  CUSTOM_B
}

extension DebugOptionMethods on DebugOption {
  /// Return the flag value for this debug option. Several flag values can be ored together.
  int get flag {
    switch (this) {
      case DebugOption.TIMESTAMPS:
        return 1 << 0;
      case DebugOption.NODES:
        return 1 << 1;
      case DebugOption.CUSTOM_A:
        return 1 << 6;
      case DebugOption.CUSTOM_B:
        return 1 << 7;
      default:
        throw ArgumentError.value(this, 'DebugOption', 'Invalid debug option');
    }
  }
}
