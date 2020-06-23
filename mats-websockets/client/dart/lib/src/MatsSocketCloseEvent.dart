import 'package:mats_socket/src/mats_socket_util.dart';

class MatsSocketCloseEvent {
  final int code;
  final String reason;
  final int outstandingInitiations;
  final dynamic nativeEvent;

  MatsSocketCloseEvent(this.code, this.reason, [this.outstandingInitiations = 0, this.nativeEvent]);

  CloseCodes get websocketCloseCode {
    return CloseCodesExtension.fromCode(code);
  }

  MatsSocketCloseCodes get type {
    return MatsSocketCloseCodesExtension.fromCode(code);
  }

  Map<String, dynamic> toJson() {
    return removeNullValues({
      'code': code,
      'reason': reason,
      'outstandingInitiations': outstandingInitiations,
      'nativeEvent': nativeEvent?.toString(),
      'websocketCloseCode': websocketCloseCode.name,
      'matsSocketCloseCode': type.name,
    });
  }
}


/// WebSocket CloseCodes used in MatsSocket, and for what. Using both standard codes, and MatsSocket-specific/defined
/// codes.
/// <p/>
/// Note: Plural "Codes" since that is what the JSR 356 Java WebSocket API {@link CloseCodes does..!}
/// Copied from MatsSocketServer.java
enum MatsSocketCloseCodes {
  /// Standard code 1008 - From Server side, Client should REJECT all outstanding and "crash"/reboot application:
  /// used when the we cannot authenticate.
  /// <p/>
  /// May also be used locally from the Client: If the PreConnectOperation return status code 401 or 403 or the
  /// WebSocket connect attempt raises error too many times (e.g. total 3x number of URLs), the MatsSocket will be
  /// "Closed Session" with this status code.
  VIOLATED_POLICY,

  /// Standard code 1011 - From Server side, Client should REJECT all outstanding and "crash"/reboot application.
  /// This is the default close code if the MatsSocket "onMessage"-handler throws anything, and may also explicitly
  /// be used by the implementation if it encounters a situation it cannot recover from.
  UNEXPECTED_CONDITION,

  /// Standard code 1012 - From Server side, Client should REISSUE all outstanding upon reconnect: used when
  /// {@link MatsSocketServer#stop(int)} is invoked. Please reconnect.
  SERVICE_RESTART,

  /// Standard code 1001 - From Client/Browser side, client should have REJECTed all outstanding: Synonym for
  /// {@link #CLOSE_SESSION}, as the WebSocket documentation states <i>"indicates that an endpoint is "going away",
  /// such as a server going down <b>or a browser having navigated away from a page.</b>"</i>, the latter point
  /// being pretty much exactly correct wrt. when to close a session. So, if a browser decides to use this code
  /// when the user navigates away and the client MatsSocket library or employing application does not catch it,
  /// we'd want to catch this as a Close Session. Notice that I've not experienced a browser that actually utilizes
  /// this close code yet, though!
  /// <p/>
  /// <b>Notice that if a close with this close code <i>is initiated from the Server-side</i>, this should NOT be
  /// considered a CLOSE_SESSION by neither the client nor the server!</b> At least Jetty's implementation of JSR
  /// 356 WebSocket API for Java sends GOING_AWAY upon socket close <i>due to timeout</i>. Since a timeout can
  /// happen if we loose connection and thus can't convey PINGs, the MatsSocketServer must not interpret Jetty's
  /// timeout-close as Close Session. Likewise, if the client just experienced massive lag on the connection, and
  /// thus didn't get the PING over to the server in a timely fashion, but then suddenly gets Jetty's timeout close
  /// with GOING_AWAY, this should not be interpreted by the client as the server wants to close the
  /// MatsSocketSession.
  GOING_AWAY,

  /// 4000: Both from Server side and Client/Browser side, client should REJECT all outstanding:
  /// <ul>
  /// <li>From Client/Browser: Used when the client closes WebSocket "on purpose", wanting to close the session -
  /// typically when the user explicitly logs out, or navigates away from web page. All traces of the
  /// MatsSocketSession are effectively deleted from the server, including any undelivered replies and messages
  /// ("push") from server.</li>
  /// <li>From Server: {@link MatsSocketServer#closeSession(String)} was invoked, and the WebSocket to that client
  /// was still open, so we close it.</li>
  /// </ul>
  CLOSE_SESSION,

  /// 4001: From Server side, Client should REJECT all outstanding and "crash"/reboot application: A
  /// HELLO:RECONNECT was attempted, but the session was gone. A considerable amount of time has probably gone by
  /// since it last was connected. The client application must get its state synchronized with the server side's
  /// view of the world, thus the suggestion of "reboot".
  SESSION_LOST,

  /// 4002: Both from Server side and from Client/Browser side: REISSUE all outstanding upon reconnect:
  /// <ul>
  /// <li>From Client: The client just fancied a little break (just as if lost connection in a tunnel), used from
  /// integration tests.</li>
  /// <li>From Server: We ask that the client reconnects. This gets us a clean state and in particular new
  /// authentication (In case of using OAuth/OIDC tokens, the client is expected to fetch a fresh token from token
  /// server).</li>
  /// </ul>
  RECONNECT,

  /// 4003: From Server side: Currently used in the specific situation where a MatsSocket client connects with the
  /// same MatsSocketSessionId as an existing WebSocket connection. This could happen if the client has realized
  /// that a connection is wonky and wants to ditch it, but the server has not realized the same yet. When the
  /// server then gets the new connect, it'll see that there is an active WebSocket already. It needs to close
  /// that. But the client "must not do anything" other than what it already is doing - reconnecting.
  DISCONNECT,

  /// 4004: From Server side: Client should REJECT all outstanding and "crash"/reboot application: Used when the
  /// client does not speak the MatsSocket protocol correctly. Session is closed.
  MATS_SOCKET_PROTOCOL_ERROR,

  /// Indicates a WebSocket close code that does not map to any specific MatsSocketCloseCode.
  /// This can occur when there WebSocket is closed by other mechanisms than vie MatsSocket, and the
  /// close code is one that is unknown to the MatsSocket library.
  UNKNOWN
}

extension MatsSocketCloseCodesExtension on MatsSocketCloseCodes {
  int get code {
    switch (this) {
      case MatsSocketCloseCodes.VIOLATED_POLICY:
        return CloseCodes.VIOLATED_POLICY.code;
      case MatsSocketCloseCodes.UNEXPECTED_CONDITION:
        return CloseCodes.UNEXPECTED_CONDITION.code;
      case MatsSocketCloseCodes.SERVICE_RESTART:
        return CloseCodes.SERVICE_RESTART.code;
      case MatsSocketCloseCodes.GOING_AWAY:
        return CloseCodes.GOING_AWAY.code;
      case MatsSocketCloseCodes.CLOSE_SESSION:
        return 4000;
      case MatsSocketCloseCodes.SESSION_LOST:
        return 4001;
      case MatsSocketCloseCodes.RECONNECT:
        return 4002;
      case MatsSocketCloseCodes.DISCONNECT:
        return 4003;
      case MatsSocketCloseCodes.MATS_SOCKET_PROTOCOL_ERROR:
        return 4004;
      case MatsSocketCloseCodes.UNKNOWN:
        return -1;
      default:
        throw ArgumentError.value(this, 'MatsSocketCloseCodes', 'No code defined for enum value');
    }
  }

  String get name {
    switch (this) {
      case MatsSocketCloseCodes.VIOLATED_POLICY:
        return 'VIOLATED_POLICY';
      case MatsSocketCloseCodes.UNEXPECTED_CONDITION:
        return 'UNEXPECTED_CONDITION';
      case MatsSocketCloseCodes.SERVICE_RESTART:
        return 'SERVICE_RESTART';
      case MatsSocketCloseCodes.GOING_AWAY:
        return 'GOING_AWAY';
      case MatsSocketCloseCodes.CLOSE_SESSION:
        return 'CLOSE_SESSION';
      case MatsSocketCloseCodes.SESSION_LOST:
        return 'SESSION_LOST';
      case MatsSocketCloseCodes.RECONNECT:
        return 'RECONNECT';
      case MatsSocketCloseCodes.DISCONNECT:
        return 'DISCONNECT';
      case MatsSocketCloseCodes.MATS_SOCKET_PROTOCOL_ERROR:
        return 'MATS_SOCKET_PROTOCOL_ERROR';
      case MatsSocketCloseCodes.UNKNOWN:
        return 'UNKNOWN';
      default:
        throw ArgumentError.value(this, 'MatsSocketCloseCodes', 'No name defined for enum value');
    }
  }

  static String nameFor(final int code) {
    var matsSocketCloseCode = fromCode(code);
    return (matsSocketCloseCode == MatsSocketCloseCodes.UNKNOWN) ? 'UNKNOWN($code)' : matsSocketCloseCode.name;
  }

  /// Convert a close code int to a ClodeCodes enum.
  static MatsSocketCloseCodes fromCode(final int code) {
    if (code == CloseCodes.VIOLATED_POLICY.code) {
      return MatsSocketCloseCodes.VIOLATED_POLICY;
    }
    else if (code == CloseCodes.UNEXPECTED_CONDITION.code) {
      return MatsSocketCloseCodes.UNEXPECTED_CONDITION;
    }
    else if (code == CloseCodes.SERVICE_RESTART.code) {
      return MatsSocketCloseCodes.SERVICE_RESTART;
    }
    else if (code == CloseCodes.GOING_AWAY.code) {
      return MatsSocketCloseCodes.GOING_AWAY;
    }
    switch (code) {
      case 4000:
        return MatsSocketCloseCodes.CLOSE_SESSION;
      case 4001:
        return MatsSocketCloseCodes.SESSION_LOST;
      case 4002:
        return MatsSocketCloseCodes.RECONNECT;
      case 4003:
        return MatsSocketCloseCodes.DISCONNECT;
      case 4004:
        return MatsSocketCloseCodes.MATS_SOCKET_PROTOCOL_ERROR;
      default:
        return MatsSocketCloseCodes.UNKNOWN;
    }
  }
}

/// An Enumeration of status codes for a web socket close that
/// are defined in the specification.
///
/// Copied from javax.websocket.CloseReason.CloseCodes
enum CloseCodes {
  /// 1000 indicates a normal closure, meaning that the purpose for
  /// which the connection was established has been fulfilled.
  NORMAL_CLOSURE,
  /// 1001 indicates that an endpoint is "going away", such as a server
  /// going down or a browser having navigated away from a page.
  GOING_AWAY,
  /// 1002 indicates that an endpoint is terminating the connection due
  /// to a protocol error.
  PROTOCOL_ERROR,
  /// 1003 indicates that an endpoint is terminating the connection
  /// because it has received a type of data it cannot accept (e.g., an
  /// endpoint that understands only text data MAY send this if it
  /// receives a binary message).
  CANNOT_ACCEPT,
  /// Reserved.  The specific meaning might be defined in the future.
  RESERVED,
  /// 1005 is a reserved value and MUST NOT be set as a status code in a
  /// Close control frame by an endpoint.  It is designated for use in
  /// applications expecting a status code to indicate that no status
  /// code was actually present.
  NO_STATUS_CODE,
  /// 1006 is a reserved value and MUST NOT be set as a status code in a
  /// Close control frame by an endpoint.  It is designated for use in
  /// applications expecting a status code to indicate that the
  /// connection was closed abnormally, e.g., without sending or
  /// receiving a Close control frame.
  CLOSED_ABNORMALLY,
  /// 1007 indicates that an endpoint is terminating the connection
  /// because it has received data within a message that was not
  /// consistent with the type of the message (e.g., non-UTF-8
  /// data within a text message).
  NOT_CONSISTENT,
  /// 1008 indicates that an endpoint is terminating the connection
  /// because it has received a message that violates its policy.  This
  /// is a generic status code that can be returned when there is no
  /// other more suitable status code (e.g., 1003 or 1009) or if there
  /// is a need to hide specific details about the policy.
  VIOLATED_POLICY,
  /// 1009 indicates that an endpoint is terminating the connection
  /// because it has received a message that is too big for it to
  /// process.
  TOO_BIG,
  /// 1010 indicates that an endpoint (client) is terminating the
  /// connection because it has expected the server to negotiate one or
  /// more extension, but the server didn't return them in the response
  /// message of the WebSocket handshake.  The list of extensions that
  /// are needed SHOULD appear in the /reason/ part of the Close frame.
  /// Note that this status code is not used by the server, because it
  /// can fail the WebSocket handshake instead.
  NO_EXTENSION,
  /// 1011 indicates that a server is terminating the connection because
  /// it encountered an unexpected condition that prevented it from
  /// fulfilling the request.
  UNEXPECTED_CONDITION,
  /// 1012 indicates that the service will be restarted.
  SERVICE_RESTART,
  /// 1013 indicates that the service is experiencing overload
  TRY_AGAIN_LATER,
  /// 1015 is a reserved value and MUST NOT be set as a status code in a
  /// Close control frame by an endpoint.  It is designated for use in
  /// applications expecting a status code to indicate that the
  /// connection was closed due to a failure to perform a TLS handshake
  /// (e.g., the server certificate can't be verified).
  TLS_HANDSHAKE_FAILURE,
  /// To handle close codes that are not defined in the spec (it could be any integer, we can't predict everything
  /// that occurs in the wild), we need to use this UNKNOWN enum value. When this occurs, we will also lose the
  /// information about which specific closeCode was used. In the library we do log the closeCode directly, rather
  /// than this resolved enum value, so this does not have any practical concerns for the library itself.
  UNKNOWN
}

extension CloseCodesExtension on CloseCodes {
  int get code {
    switch (this) {
      case CloseCodes.NORMAL_CLOSURE:
        return 1000;
      case CloseCodes.GOING_AWAY:
        return 1001;
      case CloseCodes.PROTOCOL_ERROR:
        return 1002;
      case CloseCodes.CANNOT_ACCEPT:
        return 1003;
      case CloseCodes.RESERVED:
        return 1004;
      case CloseCodes.NO_STATUS_CODE:
        return 1005;
      case CloseCodes.CLOSED_ABNORMALLY:
        return 1006;
      case CloseCodes.NOT_CONSISTENT:
        return 1007;
      case CloseCodes.VIOLATED_POLICY:
        return 1008;
      case CloseCodes.TOO_BIG:
        return 1009;
      case CloseCodes.NO_EXTENSION:
        return 1010;
      case CloseCodes.UNEXPECTED_CONDITION:
        return 1011;
      case CloseCodes.SERVICE_RESTART:
        return 1012;
      case CloseCodes.TRY_AGAIN_LATER:
        return 1013;
      case CloseCodes.TLS_HANDSHAKE_FAILURE:
        return 1015;
      case CloseCodes.UNKNOWN:
        // Since we do not know which close code we are dealing with, we do not
        // know which integer to return here either. -1 seems like a good enough compromise
        // to represent this.
        return -1;
      default:
        throw ArgumentError.value(this, 'CloseCodes', 'No code defined for enum value');
    }
  }

  String get name {
    switch (this) {
      case CloseCodes.NORMAL_CLOSURE:
        return 'NORMAL_CLOSURE';
      case CloseCodes.GOING_AWAY:
        return 'GOING_AWAY';
      case CloseCodes.PROTOCOL_ERROR:
        return 'PROTOCOL_ERROR';
      case CloseCodes.CANNOT_ACCEPT:
        return 'CANNOT_ACCEPT';
      case CloseCodes.RESERVED:
        return 'RESERVED';
      case CloseCodes.NO_STATUS_CODE:
        return 'NO_STATUS_CODE';
      case CloseCodes.CLOSED_ABNORMALLY:
        return 'CLOSED_ABNORMALLY';
      case CloseCodes.NOT_CONSISTENT:
        return 'NOT_CONSISTENT';
      case CloseCodes.VIOLATED_POLICY:
        return 'VIOLATED_POLICY';
      case CloseCodes.TOO_BIG:
        return 'TOO_BIG';
      case CloseCodes.NO_EXTENSION:
        return 'NO_EXTENSION';
      case CloseCodes.UNEXPECTED_CONDITION:
        return 'UNEXPECTED_CONDITION';
      case CloseCodes.SERVICE_RESTART:
        return 'SERVICE_RESTART';
      case CloseCodes.TRY_AGAIN_LATER:
        return 'TRY_AGAIN_LATER';
      case CloseCodes.TLS_HANDSHAKE_FAILURE:
        return 'TLS_HANDSHAKE_FAILURE';
      case CloseCodes.UNKNOWN:
        return 'UNKNOWN';
      default:
        throw ArgumentError.value(this, 'CloseCodes', 'No name defined for enum value');
    }
  }

  static
  /// Convert a code int to a CloseCodes enum.
  CloseCodes fromCode(final int code) {
    switch (code) {
      case 1000:
        return CloseCodes.NORMAL_CLOSURE;
      case 1001:
        return CloseCodes.GOING_AWAY;
      case 1002:
        return CloseCodes.PROTOCOL_ERROR;
      case 1003:
        return CloseCodes.CANNOT_ACCEPT;
      case 1004:
        return CloseCodes.RESERVED;
      case 1005:
        return CloseCodes.NO_STATUS_CODE;
      case 1006:
        return CloseCodes.CLOSED_ABNORMALLY;
      case 1007:
        return CloseCodes.NOT_CONSISTENT;
      case 1008:
        return CloseCodes.VIOLATED_POLICY;
      case 1009:
        return CloseCodes.TOO_BIG;
      case 1010:
        return CloseCodes.NO_EXTENSION;
      case 1011:
        return CloseCodes.UNEXPECTED_CONDITION;
      case 1012:
        return CloseCodes.SERVICE_RESTART;
      case 1013:
        return CloseCodes.TRY_AGAIN_LATER;
      case 1015:
        return CloseCodes.TLS_HANDSHAKE_FAILURE;
      default:
        return CloseCodes.UNKNOWN;
    }
  }
}