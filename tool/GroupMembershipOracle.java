import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.common.protocol.*;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;

/** Offline official Kafka 4.0.2 serialization oracle; no broker required for fixtures. */
public final class GroupMembershipOracle {
  static final Map<String,String> fixtures = new LinkedHashMap<>();
  static void put(String name, ByteBuffer bytes) {
    byte[] data = new byte[bytes.remaining()]; bytes.get(data);
    fixtures.put(name, HexFormat.of().formatHex(data));
  }
  static void put(String name, Message message, int version) {
    put(name, MessageUtil.toByteBuffer(message, (short)version));
  }
  static void fixtures() {
    byte[] metadata = {0,1,2,(byte)255};
    var protocols = new JoinGroupRequestData.JoinGroupRequestProtocolCollection();
    protocols.add(new JoinGroupRequestData.JoinGroupRequestProtocol().setName("range").setMetadata(metadata));
    put("joinRequestV4", new JoinGroupRequestData().setGroupId("grπ").setSessionTimeoutMs(10000)
      .setRebalanceTimeoutMs(20000).setMemberId("m").setProtocolType("consumer").setProtocols(protocols),4);
    put("joinResponseV4",new JoinGroupResponseData().setThrottleTimeMs(17).setErrorCode((short)0)
      .setGenerationId(7).setProtocolName("range").setLeader("m").setMemberId("m")
      .setMembers(List.of(new JoinGroupResponseData.JoinGroupResponseMember().setMemberId("m").setMetadata(metadata))),4);
    put("syncRequestV2",new SyncGroupRequestData().setGroupId("grπ").setGenerationId(7).setMemberId("m")
      .setAssignments(List.of(new SyncGroupRequestData.SyncGroupRequestAssignment().setMemberId("m").setAssignment(metadata))),2);
    put("syncResponseV2",new SyncGroupResponseData().setThrottleTimeMs(18).setErrorCode((short)0).setAssignment(metadata),2);
    put("heartbeatRequestV2",new HeartbeatRequestData().setGroupId("grπ").setGenerationId(7).setMemberId("m"),2);
    put("heartbeatResponseV2",new HeartbeatResponseData().setThrottleTimeMs(19).setErrorCode((short)22),2);
    put("leaveRequestV2",new LeaveGroupRequestData().setGroupId("grπ").setMemberId("m"),2);
    put("leaveResponseV2",new LeaveGroupResponseData().setThrottleTimeMs(20).setErrorCode((short)25),2);
    put("commitRequestV6",new OffsetCommitRequestData().setGroupId("grπ").setGenerationIdOrMemberEpoch(7).setMemberId("m")
      .setTopics(List.of(new OffsetCommitRequestData.OffsetCommitRequestTopic().setName("tπ")
        .setPartitions(List.of(new OffsetCommitRequestData.OffsetCommitRequestPartition().setPartitionIndex(2)
          .setCommittedOffset(123).setCommittedLeaderEpoch(-1).setCommittedMetadata("savedπ"))))),6);
    for(short v=0;v<=3;v++) {
      var sub = new ConsumerPartitionAssignor.Subscription(List.of("tπ"),ByteBuffer.wrap(metadata),
        List.of(new TopicPartition("tπ",2)),7,Optional.of("rackπ"));
      put("subscriptionV"+v,ConsumerProtocol.serializeSubscription(sub,v));
      put("assignmentV"+v,ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(
        List.of(new TopicPartition("tπ",2)),ByteBuffer.wrap(metadata)),v));
      put("subscriptionNullV"+v,ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(List.of("tπ")),v));
      put("assignmentNullV"+v,ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(List.of(new TopicPartition("tπ",2))),v));
    }
    System.out.println("{"); int i=0;
    for(var e:fixtures.entrySet()) System.out.println("  \""+e.getKey()+"\": \""+e.getValue()+"\""+(++i<fixtures.size()?",":""));
    System.out.println("}");
  }
  // A bounded classic RangeAssignor peer. The caller owns and joins the process.
  static void peer(String[] args) {
    Properties p=new Properties();
    p.put("bootstrap.servers",args[1]); p.put("group.id",args[2]);
    p.put("group.protocol","classic"); p.put("partition.assignment.strategy",RangeAssignor.class.getName());
    p.put("enable.auto.commit","false"); p.put("auto.offset.reset","earliest");
    p.put("key.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
    p.put("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
    p.put("session.timeout.ms","10000"); p.put("heartbeat.interval.ms","1000");
    p.put("max.poll.interval.ms","20000"); p.put("default.api.timeout.ms","20000");
    p.put("request.timeout.ms","15000"); p.put("allow.auto.create.topics","false");
    try(KafkaConsumer<byte[],byte[]> c=new KafkaConsumer<>(p)) {
      c.subscribe(List.of(args[3]),new ConsumerRebalanceListener(){
        public void onPartitionsRevoked(Collection<TopicPartition> ps) { System.out.println("ORACLE_REVOKED"); System.out.flush(); }
        public void onPartitionsAssigned(Collection<TopicPartition> ps) {
          System.out.println("ORACLE_ASSIGNED "+c.groupMetadata().generationId()+" "+ps.size()); System.out.flush();
        }
      });
      long deadline=System.nanoTime()+Duration.ofSeconds(45).toNanos();
      while(System.nanoTime()<deadline) c.poll(Duration.ofMillis(200));
    }
  }
  public static void main(String[] args) {
    if(args.length==0) fixtures();
    else if(args.length==4 && args[0].equals("peer")) peer(args);
    else throw new IllegalArgumentException("[peer bootstrap group topic]");
  }
}
