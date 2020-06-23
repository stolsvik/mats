import 'package:mats_socket/src/mats_socket_util.dart';

/// Information about how a subscription went on the server side. If you do two subscriptions to the same Topic,
/// you will still only get one such message - thus if you want one for each, you'd better add two listeners too,
/// <i>before</i> doing any of the subscribes.
/// <p />
/// Note: this also fires upon every reconnect. <b>Make note of the {@link SubscriptionEventType#LOST_MESSAGES}!</b>
///
/// @param type {SubscriptionEventType}
/// @param topicId {string}
/// @constructor
class SubscriptionEvent {
  /// How the subscription fared.
  ///
  /// @type {SubscriptionEventType}
  final SubscriptionEventType type;

  /// What TopicIc this relates to.
  ///
  /// @type {string}
  final String topicId;

  const SubscriptionEvent(this.type, this.topicId);


  Map<String, dynamic> toJson() {
    return removeNullValues({
      'type': type.name,
      'topicId': topicId
    });
  }
}

/// Type of {@link SubscriptionEvent}.
///
/// @enum {string}
/// @readonly
enum SubscriptionEventType {
  /// The subscription on the server side went ok. If reconnect, any missing messages are now being sent.
  OK,

  /// You were not authorized to subscribe to this Topic.
  NOT_AUTHORIZED,

  /// Upon reconnect, the "last message Id" was not known to the server, implying that there are lost messages.
  /// Since you will now have to handle this situation by other means anyway (e.g. do a request for all stock ticks
  /// between the last know timestamp and now), you will thus not get any of the lost messages even if the server
  /// has some.
  LOST_MESSAGES
}

extension SubscriptionEventTypeExtension on SubscriptionEventType {
  String get name {
    switch (this) {
      case SubscriptionEventType.OK:
        return 'OK';
      case SubscriptionEventType.NOT_AUTHORIZED:
        return 'NOT_AUTHORIZED';
      case SubscriptionEventType.LOST_MESSAGES:
        return 'LOST_MESSAGES';
      default:
        throw ArgumentError.value(this, 'SubscriptionEventType', 'Unknown enum value');
    }
  }
}