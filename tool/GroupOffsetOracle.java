import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/** Official-client oracle. Manual assignment only; never joins or auto commits. */
public final class GroupOffsetOracle {
  public static void main(String[] args) {
    if (args.length != 5 && args.length != 7)
      throw new IllegalArgumentException("read bootstrap group topic partition | commit bootstrap group topic partition nextOffset base64Metadata");
    Properties p = new Properties();
    p.put("bootstrap.servers", args[1]);
    p.put("group.id", args[2]);
    p.put("enable.auto.commit", "false");
    p.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    p.put("default.api.timeout.ms", "20000");
    p.put("request.timeout.ms", "10000");
    p.put("allow.auto.create.topics", "false");
    TopicPartition tp = new TopicPartition(args[3], Integer.parseInt(args[4]));
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(p)) {
      consumer.assign(Collections.singleton(tp));
      if (args[0].equals("commit") && args.length == 7) {
        String metadata = new String(Base64.getDecoder().decode(args[6]), StandardCharsets.UTF_8);
        consumer.commitSync(Collections.singletonMap(tp,
            new OffsetAndMetadata(Long.parseLong(args[5]), metadata)), Duration.ofSeconds(20));
        System.out.println("ORACLE_COMMITTED");
      } else if (args[0].equals("read") && args.length == 5) {
        Map<TopicPartition, OffsetAndMetadata> offsets =
            consumer.committed(Collections.singleton(tp), Duration.ofSeconds(20));
        OffsetAndMetadata value = offsets.get(tp);
        if (value == null) {
          System.out.println("ORACLE_ABSENT");
        } else {
          System.out.println("ORACLE_OFFSET " + value.offset() + " "
              + value.leaderEpoch().orElse(-1) + " "
              + Base64.getEncoder().encodeToString(value.metadata().getBytes(StandardCharsets.UTF_8)));
        }
      } else {
        throw new IllegalArgumentException("Unsupported oracle operation");
      }
    }
  }
}
