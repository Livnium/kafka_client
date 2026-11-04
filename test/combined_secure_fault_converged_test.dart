// Application-coordinated candidate only. The immediate-Fetch gate stays separate.
import 'combined_secure_fault_integration_test.dart' as combined;

void main() => combined.runCombinedSecureFaultTest(
      allowTlsBootstrapFallback: true,
      awaitMetadataElection: true,
    );
