import 'MatsSocket.dart';
import 'mats_socket_util.dart';

/// Method signature for listeners wishing to accept [ConnectionEvent]
typedef ConnectionEventListener = Function(ConnectionEvent);

/// Event object for [MatsSocket.addConnectionEventListener].
///
/// **Note on event ordering**: [ConnectionEvent]s are delivered ASAP. This means that for events that the
/// client controls, they are issued *before* the operation they describe commences:
/// [ConnectionEventType.CONNECTING] and
/// [ConnectionEventType.SESSION_ESTABLISHED]. However, for events where the client is
/// "reacting", e.g. when the WebSocket connects, or abruptly closes, they are issued ASAP when the Client gets to know about it:
/// [ConnectionEventType.CONNECTED], [ConnectionEventType.LOST_CONNECTION],
/// [ConnectionEventType.CONNECTION_ERROR] and [ConnectionEventType.WAITING].
/// For [ConnectionEventType.COUNTDOWN], there is not much to say wrt. timing, other than you won't typically
/// get a 'countdown'-event with 0 seconds left, as that is when we transition into 'connecting' again. For events
/// that also describe [ConnectionState]s, the [MatsSocket.state] is updated before the event is fired.
class ConnectionEvent {
  /// The type of the `ConnectionEvent`, returns an enum value of [ConnectionEventType].
  final ConnectionEventType type;

  /// For all of the events this holds the current URL we're either connected to, was connected to, or trying to
  /// connect to.
  final Uri webSocketUrl;

  /// For several of the events (enumerated in [ConnectionEventType]), there is an underlying WebSocket event
  /// that caused it. This field holds that.
  ///
  ///     * [ConnectionEventType.WAITING]: WebSocket [CloseEvent] that caused this transition.
  ///     * [ConnectionEventType.CONNECTED]: WebSocket [Event] that caused this transition.
  ///     * [ConnectionEventType.CONNECTION_ERROR]: WebSocket [Event] that caused this transition.
  ///     * [ConnectionEventType.LOST_CONNECTION]: WebSocket [CloseEvent] that caused it.
  ///
  final dynamic webSocketEvent;

  /// For [ConnectionEventType.CONNECTING], [ConnectionEventType.WAITING] and [ConnectionEventType.COUNTDOWN],
  /// tells how long the timeout for this attempt is, i.e. what the COUNTDOWN events start out with. Together with
  /// [countdownSeconds] of the COUNTDOWN events, this can be used to calculate a fraction if you want to
  /// make a "progress bar" of sorts.
  ///
  /// The timeouts starts at 500 ms (unless there is only 1 URL configured, in which case 5 seconds), and then
  /// increases exponentially, but maxes out at 15 seconds.
  final Duration timeout;

  /// For [ConnectionEventType.CONNECTING], [ConnectionEventType.WAITING] and [ConnectionEventType.COUNTDOWN],
  /// tells how long since this last attempt transitioned to [ConnectionEventType.CONNECTING]. This will count
  /// up to [timeout], at which point the connection attempt will be closed and restarted.
  ///
  /// A number to present to the user is present in [countdownSeconds], which can be used to show how much time
  /// is left of the current attempt.
  final Duration elapsed;

  /// For [ConnectionEventType.CONNECTING], [ConnectionEventType.WAITING] and [ConnectionEventType.COUNTDOWN],
  /// tells how many seconds there are left for this attempt (of the [timeout] it started with),
  /// with a tenth of a second as precision. With the COUNTDOWN events, these come in each 100 ms (1/10 second),
  /// and show how long time there is left before trying again (if MatsSocket is configured with multiple URLs,
  /// the next attempt will be a different URL).
  ///
  /// The countdown is started when the state transitions to [ConnectionEventType.CONNECTING], and
  /// stops either when [ConnectionEventType.CONNECTED] or the timeout reaches zero. If the
  /// state is still CONNECTING when the countdown reaches zero, implying that the "new WebSocket(..)" call still
  /// has not either opened or closed, the connection attempt is aborted by calling webSocket.close(). It then
  /// tries again, possibly with a different URL - and the countdown starts over.
  ///
  /// Notice that the countdown is not affected by any state transition into [ConnectionEventType.WAITING] -
  /// such transition only means that the "new WebSocket(..)" call failed and emitted a close-event, but we will
  /// still wait out the countdown before trying again.
  ///
  /// Notice that you will most probably not get an event with 0 seconds, as that is when we transition into
  /// [ConnectionEventType.CONNECTING] and the countdown starts over (possibly with a larger timeout).
  ///
  /// Truncated exponential backoff: The timeouts starts at 500 ms (unless there is only 1 URL configured, in which
  /// case 5 seconds), and then increases exponentially, but maxes out at 15 seconds.
  String get countdownSeconds {
    if (timeout == null || elapsed == null) {
      return '';
    }
    var countdown = timeout - elapsed;
    // Round down the elapsed microseconds to deci seconds (10th of a second)
    var deciSeconds = (countdown.inMicroseconds / 100000).round();
    var seconds = (deciSeconds / 10).floor();
    var tenthSeconds = deciSeconds % 10;
    return '${seconds}.${tenthSeconds}';
  }

  /// The connection attempt count, starts at 0th attempt and increases for each time the connection attempt fails.
  final int connectionAttempt;

  const ConnectionEvent(this.type, this.webSocketUrl, this.webSocketEvent, [this.timeout, this.elapsed, this.connectionAttempt]);

  Map<String, dynamic> toJson() {
    return removeNullValues({
      'type': type.name,
      'webSocketUrl': webSocketUrl.toString(),
      'webSocketEvent': webSocketEvent?.toString(),
      'timeoutMs': timeout?.inMilliseconds,
      'elapsedMs': elapsed?.inMilliseconds,
      'connectionAttempt': connectionAttempt,
    });
  }
}

/// The event types of [ConnectionEvent] - four of the event types are state-transitions into different states
/// of [ConnectionState].
///
enum ConnectionEventType {
  /// State, and fires as ConnectionEvent when we transition into this state, which is when the WebSocket is literally trying to connect.
  /// This is between [:new WebSocket(url):] (or the [MatsSocket.preconnectoperation] if configured),
  /// and either webSocket.onopen or webSocket.onclose is fired, or countdown reaches 0. If webSocket.onopen,
  /// we transition into [CONNECTED], if webSocket.onclose, we transition into
  /// [WAITING]. If we reach countdown 0 while in CONNECTING, we will "re-transition" to the same state, and
  /// thus get one more event of CONNECTING.
  ///
  /// User Info Tip: Show a info-box, stating "Connecting! <4.0 seconds..>", countdown in "grayed out" style, box is
  /// some neutral information color, e.g. yellow (fading over to this color if already red or orange due to
  /// [CONNECTION_ERROR] or [LOST_CONNECTION]).
  /// Each time it transitions into CONNECTING, it will start a new countdown. Let's say it starts from say 4
  /// seconds: If this connection attempt fails after 1 second, it will transition into WAITING and continue the
  /// countdown with 3 seconds remaining.
  CONNECTING,

  /// State, and fires as ConnectionEvent when we transition into this state, which is when [CONNECTING] fails.
  /// The only transition out of this state is [CONNECTING], when the [COUNTDOWN] reaches 0.
  ///
  /// Notice that the [ConnectionEvent] contains the [:Event:] that came with webSocket.close (while CONNECTING).
  ///
  /// User Info Tip: Show a info-box, stating "Waiting! <2.9 seconds..>", countdown in normal visibility, box is
  /// some neutral information color, e.g. yellow (keeping the box color fading if in progress).
  /// It will come into this state from [CONNECTING], and have the time remaining from the initial countdown.
  /// So if the attempt countdown started from 4 seconds, and it took 1 second before the connection attempt failed,
  /// then there will be 3 seconds left in WAITING state.
  WAITING,

  /// State, and fires as ConnectionEvent when we transition into this state, which is when WebSocket.onopen is fired.
  /// Notice that the MatsSocket is still not fully established, as we have not yet exchanged HELLO and WELCOME -
  /// the MatsSocket is fully established at [SESSION_ESTABLISHED].
  ///
  /// Notice that the [ConnectionEvent] contains the WebSocket 'onopen' [:Event:] that was issued when
  /// the WebSocket opened.
  ///
  /// User Info Tip: Show a info-box, stating "Connected!", happy-color, e.g. green, with no countdown.
  CONNECTED,

  /// State, and fires as ConnectionEvent when we transition into this state, which is when when the WELCOME MatsSocket message comes
  /// from the Server, also implying that it has been authenticated: The MatsSocket is now fully established, and
  /// actual messages can be exchanged.
  ///
  /// User Info Tip: Show a info-box, stating "Session OK!", happy-color, e.g. green, with no countdown - and the
  /// entire info-box fades away fast, e.g. after 1 second.
  SESSION_ESTABLISHED,

  /// This is a pretty worthless event. It comes from WebSocket.onerror. It will *always* be trailed by a
  /// WebSocket.onclose, which gives the event [LOST_CONNECTION].
  ///
  /// Notice that the [ConnectionEvent] contains the [:Event:] that caused the error.
  ///
  /// User Info Tip: Show a info-box, which is some reddish color (no need for text since next event [LOST_CONNECTION]) comes immediately).
  CONNECTION_ERROR,

  /// This comes when WebSocket.onclose is fired "unexpectedly", **and the reason for this close is NOT a SessionClosed Event** (The latter will
  /// instead invoke the listeners registered with [MatsSocket.addSessionClosedEventListener(listener)]).
  /// A LOST_CONNECTION will start a reconnection attempt after a very brief delay (couple of hundred milliseconds),
  /// and the next state transition and thus event is [CONNECTING].
  ///
  /// Notice that the [ConnectionEvent] contains the [:CloseEvent:] that caused the lost connection.
  ///
  /// User Info Tip: Show a info-box, stating "Connection broken!", which is some orange color (unless it already
  /// is red due to [CONNECTION_ERROR]), fading over to the next color when next event ([CONNECTING]
  /// comes in.
  LOST_CONNECTION,

  /// Events fired every 100ms while in state [CONNECTING], possibly continuing over to [WAITING].
  /// Notice that you will most probably not get an event with 0 seconds left, as that is when we (re-)transition to
  /// [CONNECTING] and the countdown starts over (possibly with a larger timeout). Read more at
  /// [ConnectionEvent.countdownSeconds].
  ///
  /// User Info Tip: Read more at [CONNECTING] and [WAITING].
  COUNTDOWN,
}

extension ConnectionEventTypeExtension on ConnectionEventType {
  String get name {
    switch (this) {
      case ConnectionEventType.CONNECTING:
        return 'CONNECTING';
      case ConnectionEventType.CONNECTION_ERROR:
        return 'CONNECTION_ERROR';
      case ConnectionEventType.LOST_CONNECTION:
        return 'LOST_CONNECTION';
      case ConnectionEventType.COUNTDOWN:
        return 'COUNTDOWN';
      case ConnectionEventType.WAITING:
        return 'WAITING';
      case ConnectionEventType.CONNECTED:
        return 'CONNECTED';
      case ConnectionEventType.SESSION_ESTABLISHED:
        return 'SESSION_ESTABLISHED';
      default:
        throw ArgumentError.value(this, 'ConnectionEventType', 'Unknown enum value');
    }
  }

  ConnectionState get connectionState {
    switch (this) {
      case ConnectionEventType.CONNECTING:
        return ConnectionState.CONNECTING;
      case ConnectionEventType.WAITING:
        return ConnectionState.WAITING;
      case ConnectionEventType.CONNECTED:
        return ConnectionState.CONNECTED;
      case ConnectionEventType.SESSION_ESTABLISHED:
        return ConnectionState.SESSION_ESTABLISHED;
      default:
        return null;
    }
  }
}

/// States for MatsSocket's {@link MatsSocket#state state}.
enum ConnectionState {
  /// This is the initial State of a MatsSocket. Also, the MatsSocket is re-set back to this State in a
  /// Session-Closed-from-Server situation (which is communicated via listeners registered with
  /// {@link MatsSocket#addSessionClosedEventListener(listener)}), OR if you have explicitly performed a
  /// matsSocket.close().
  ///
  /// Only transition out of this state is into {@link #CONNECTING}.
  NO_SESSION,

  /// Read doc at {@link ConnectionEventType#CONNECTING}.
  CONNECTING,

  /// Read doc at {@link ConnectionEventType#WAITING}.
  WAITING,

  /// Read doc at {@link ConnectionEventType#CONNECTED}.
  CONNECTED,

  /// Read doc at {@link ConnectionEventType#SESSION_ESTABLISHED}.
  SESSION_ESTABLISHED
}
