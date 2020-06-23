import 'MatsSocket.dart';
import 'mats_socket_util.dart';

/// Signature for callback of [AuthorizationRequiredEvent].
///
/// This callback is registered with a MatsSocket to provide authorizations via [MatsSocket.setCurrentAuthorization].
typedef AuthorizationExpiredCallback = Function(AuthorizationRequiredEvent);

/// Event sent to [AuthorizationExpiredCallback] when a new authorization is required.
class AuthorizationRequiredEvent {
  /// Type of the event, one of {@link AuthorizationRequiredEvent}.
  final AuthorizationRequiredEventType type;

  /// When the current Authorization expires - note that this might well still be in the future,
  /// but the "slack" left before expiration is used up.
  final DateTime currentExpiration;

  /// [type] type type of the event, one of [AuthorizationRequiredEvent].
  /// [currentExpiration] when the current Authorization expires.
  AuthorizationRequiredEvent(this.type, [this.currentExpiration]);

  Map<String, dynamic> toJson() {
    return removeNullValues({
      'type': type.name,
      'currentExpiration': currentExpiration?.millisecondsSinceEpoch
    });
  }
}

/// Type of [AuthorizationRequiredEvent].
enum AuthorizationRequiredEventType {
  /// Initial state, if auth not already set by app.
  NOT_PRESENT,

  /// The authentication is expired - note that this might well still be in the future,
  /// but the "slack" left before expiration is not long enough.
  EXPIRED,

  /// The server has requested that the app provides fresh auth to proceed - this needs to be fully fresh, even
  /// though there might still be "slack" enough left on the current authorization to proceed. (The server side
  /// might want the full expiry to proceed, or wants to ensure that the app can still produce new auth - i.e.
  /// it might suspect that the current authentication session has been invalidated, and need proof that the app
  /// can still produce new authorizations/tokens).
  REAUTHENTICATE,
}

extension AuthorizationRequiredEventTypeExtension on AuthorizationRequiredEventType {
  String get name {
    switch (this) {
      case AuthorizationRequiredEventType.EXPIRED:
        return 'EXPIRED';
      case AuthorizationRequiredEventType.NOT_PRESENT:
        return 'NOT_PRESENT';
      case AuthorizationRequiredEventType.REAUTHENTICATE:
        return 'REAUTHENTICATE';
      default:
        throw ArgumentError.value(this, 'ConnectionEventType', 'Unknown enum value');
    }
  }
}