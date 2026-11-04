// Opt-in candidate only; default-policy gate and retained failures stay separate.
import 'combined_secure_fault_integration_test.dart' as combined;

void main() =>
    combined.runCombinedSecureFaultTest(allowTlsBootstrapFallback: true);
