// Isolated Fetch-only policy gate: no application metadata convergence wait.
import 'package:kafka_client/kafka_protocol.dart';

import 'combined_secure_fault_integration_test.dart' as combined;

void main() => combined.runCombinedSecureFaultTest(
      allowTlsBootstrapFallback: true,
      tlsFetchRecovery: KafkaTlsFetchRecovery(
        maxAttempts: 20,
        window: const Duration(seconds: 10),
        retryDelay: const Duration(milliseconds: 500),
      ),
    );
