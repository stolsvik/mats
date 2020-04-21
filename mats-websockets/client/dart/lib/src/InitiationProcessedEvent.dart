import 'package:mats_socket/src/MessageEvent.dart';
import 'MatsSocket.dart';

typedef InitiationProcessedEventListener = Function(InitiationProcessedEvent);

/// (Metrics) Information about Client-to-Server SENDs and REQUESTs (aka *Client Initiations*), including
/// experienced round-trip times for both Received acknowledgement, and for Requests, the Request-to-Reply time.
///
/// For each message that, for sends, has been acknowledged received, and for requests, has been replied to, gives
/// this information:
///
///   * Client MessageId  (envelope's 'cmid').
///   * Timestamp of when message was sent.
///   * Target MatsSocket Endpoint or Terminator Id  (envelope's 'eid').
///   * TraceId for the SEND or REQUEST  (envelope's 'tid').
///   * The outgoing message, i.e. the SEND or the REQUEST message  (envelope's 'msg').
///   * Experienced Received Acknowledge round-trip time.
///   * For [MatsSocket.request], the Reply's [MessageEventType]
///   * For [MatsSocket.requestReplyTo] Requests, the replyToTerminatorId.
///   * For Requests, the total experienced Request-to-Reply time.
///   * For Requests, the Reply [MessageEvent] object.
///
/// You may "subscribe" to <code>InitiationProcessedEvents</code> using
/// [MatsSocket.addInitiationProcessedEventListener], and you may get the latest such events from the
/// property [MatsSocket.initiations].
///
/// **Note on event ordering**:
///
///   * send: First [ReceivedEvent] is issued. Then nn [InitiationProcessedEvent] is added to
///   [MatsSocket.initiations], and then all [InitiationProcessedEvent] listeners are invoked
///   * request/requestReplyTo: First [ReceivedEvent] is issued (i.e. ack/nack), then when the reply
///   comes back to the server, an [InitiationProcessedEvent] is added to [MatsSocket.initiations], and
///   then all [InitiationProcessedEvent] listeners are invoked, and finally the [MessageEvent] is
///   delivered, either as settling of the return Reply-Promise (for 'request'), or invocation of the Terminator's
///   message- or rejectCallbacks (for 'requestReplyTo').
///
class InitiationProcessedEvent {
  /// Which initiation type of this flow, enum of [InitiationProcessedEventType].
  InitiationProcessedEventType type;

  /// Target Server MatsSocket Endpoint or Terminator Id  (envelope's 'eid').
  final String endpointId;

  /// The Client MessageId of the Initiation  (envelope's 'cmid'). For this particular MatsSocket library, this
  /// is currently an integer sequence id.
  final String clientMessageId;

  /// Millis-from-epoch when this initiation was sent.
  final DateTime sentTimestamp;

  /// The number of milliseconds offset for sending this message from the initial [ConnectionEventType.SESSION_ESTABLISHED] event for
  /// this MatsSocket - **this number will typically be negative for the first messages**: A negative number
  /// implies that the message was sent before the WELCOME was received, which again implies that the very first
  /// message will by definition have a negative offset since it is this message that starts the HELLO/WELCOME
  /// handshake and is thus enqueued before the WELCOME has been received. This is desirable: Upon application
  /// startup, stack up all requests that you need answer for to show the initial screen, and they will all be
  /// sent in a single pipeline, directly trailing the HELLO, their answers coming in as soon as possible after
  /// the WELCOME.
  ///
  /// **Note that this number can be a float, not necessarily integer**.
  final double sessionEstablishedOffsetMillis;

  /// TraceId for the initiation - which follows through all parts of the processing  (envelope's 'tid').
  final String traceId;

  /// The message object that was sent with the initiation, i.e. on send(), request() or requestReplyTo()  (outgoing envelope's 'msg').
  final dynamic initiationMessage;

  /// The experienced round-trip time for the Received Acknowledgement - this is the time back-and-forth.
  ///
  /// **Note that this number can be a float, not necessarily integer**.
  final double acknowledgeRoundTripMillis;

  // === For Requests.

  /// The [MessageEventType] for Replies to Request Initiations.
  final MessageEventType replyMessageEventType;

  /// The 'replyToTerminatorId' for [MatsSocket.requestReplyTo]-Requests.
  final String replyToTerminatorId;

  /// The experienced round-trip time from a Request initiation to the Reply (RESOLVE or REJECT) comes back.
  ///
  /// **Note that this number can be a float, not necessarily integer**.
  final double requestReplyRoundTripMillis;

  /// The Reply [MessageEvent] that was supplied to the Promise (on resolve/then or reject/catch) or ReplyTo
  /// Client [MatsSocket.terminator].
  final MessageEvent replyMessageEvent;

  InitiationProcessedEvent(
      this.endpointId,
      this.clientMessageId,
      this.sentTimestamp,
      this.sessionEstablishedOffsetMillis,
      this.traceId,
      this.initiationMessage,
      this.acknowledgeRoundTripMillis,
      this.replyMessageEventType,
      this.replyToTerminatorId,
      this.requestReplyRoundTripMillis,
      this.replyMessageEvent) {
    if (replyToTerminatorId != null) {
      type = InitiationProcessedEventType.REQUEST_REPLY_TO;
    } else if (replyMessageEventType != null) {
      type = InitiationProcessedEventType.REQUEST;
    } else {
      type = InitiationProcessedEventType.SEND;
    }
  }
}

/// Type of [InitiationProcessedEvent] - the type of the *initiation* of a flow, which also
/// determines which fields of the <code>InitiationProcessedEvent</code> are set.
enum InitiationProcessedEventType {
  /// Flow initiated with [MatsSocket.send]. Fields whose name does not start with "reply" or "request"
  /// will be set.
  SEND,

  /// Flow initiated with [MatsSocket.request]. Will have all fields except
  /// [InitiationProcessedEvent.replyToTerminatorId] set.
  REQUEST,

  /// Flow initiated with [MatsSocket.requestReplyTo]. Will have *all* fields set.
  REQUEST_REPLY_TO,
}
