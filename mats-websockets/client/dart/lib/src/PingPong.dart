/// (Metric) A "holding struct" for pings and their experienced round-trip times - you may "subscribe" to ping results
/// using {@link MatsSocket#addPingListener()}, and you may get the latest pings from the property
/// {@link MatsSocket#pings}.
class PingPong {
  /// Sequence of the ping.
  final String pingId;

  /// Millis-from-epoch when this ping was sent.
  final DateTime sentTimestamp;

  /// The experienced round-trip time for this ping-pong - this is the time back-and-forth.
  ///
  /// <b>Note that this number can be a float, not necessarily integer</b>.
  double roundTripMillis;

  PingPong(this.pingId, this.sentTimestamp);
}