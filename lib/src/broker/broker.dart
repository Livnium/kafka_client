import 'dart:async';

import '../api/abstract/kafka_request.dart';

/// Kafka TCP endpoint used for bootstrap or partition-leader routing.
///
/// TLS and optional SASL authentication are selected on the owning client.
/// For TLS, the hostname must match the broker certificate.
class Broker {
  /// DNS hostname or IP address, without a URI scheme.
  final String host;

  /// TCP listener port.
  final int port;

  /// Creates an endpoint; connection establishment happens when a request is sent.
  Broker(this.host, this.port);
}

/// Legacy request/completer holder retained for source compatibility.
///
/// This is not the active client transport's in-flight request representation.
class RequestData {
  /// Completer associated with the request result.
  final Completer<dynamic> completer;

  /// Request associated with [completer].
  final KafkaRequest request;

  /// Creates a holder without sending or scheduling the request.
  RequestData(this.completer, this.request);
}
