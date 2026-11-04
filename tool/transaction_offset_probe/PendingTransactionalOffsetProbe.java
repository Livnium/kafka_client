import java.io.*;
import java.nio.file.*;
import java.security.*;
import java.security.cert.CertificateFactory;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;

/** Owned synthetic fixture only. Wire reads have no automatic retry; producer uses official client semantics. */
public final class PendingTransactionalOffsetProbe {
  static final long BASELINE=10, PENDING=20;
  static boolean hold;
  static void emit(String event, String group, String mode, String resolution) throws Exception {
    OffsetFetchWireOracle.emit(OffsetFetchWireOracle.JSON.createObjectNode().put("event",event)
      .put("utc",Instant.now().toString()).put("group",group).put("securityProtocol",mode).put("resolution",resolution));
  }
  static ObjectNode fetch(String endpoint,String group,String topic,int version,boolean stable,String stage) throws Exception {
    ObjectNode c=OffsetFetchWireOracle.JSON.createObjectNode().put("endpoint",endpoint).put("group",group)
      .put("topic",topic).put("version",version).put("requireStable",stable);
    c.putArray("partitions").add(0);
    ObjectNode r=OffsetFetchWireOracle.fetch(c); r.put("stage",stage).put("group",group);
    OffsetFetchWireOracle.emit(r); return r;
  }
  static void expect(ObjectNode r,int error,long offset) {
    if(!r.path("groupError").asText().equals("NONE") || r.path("offsets").size()!=1
      || r.path("offsets").get(0).path("errorCode").asInt()!=error
      || r.path("offsets").get(0).path("offset").asLong()!=offset)
      throw new AssertionError("Unexpected raw OffsetFetch receipt at " + r.path("stage").asText());
  }
  static Properties props(String ep,String ca,String mode) {
    Properties p=new Properties();
    p.put("bootstrap.servers",ep); p.put("security.protocol",mode);
    p.put("ssl.truststore.type","PEM");p.put("ssl.truststore.location",ca);
    p.put("ssl.endpoint.identification.algorithm","https");p.put("request.timeout.ms","10000");
    p.put("default.api.timeout.ms","30000");
    if(mode.equals("SASL_SSL")) {
      p.put("sasl.mechanism","PLAIN");
      p.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\""+
        System.getenv("KAFKA_SECURE_USERNAME")+"\" password=\""+System.getenv("KAFKA_SECURE_PASSWORD")+"\";");
    }
    return p;
  }
  static void run(String mode,String resolution,String ca,String topic) throws Exception {
    String ep="localhost:"+System.getenv(mode.equals("SSL")?"KAFKA_SECURE_TLS_PORT":"KAFKA_SECURE_SASL_PORT");
    String group=topic+"-txn-"+mode.toLowerCase(Locale.ROOT)+"-"+resolution+"-"+UUID.randomUUID();
    Properties p=props(ep,ca,mode); TopicPartition tp=new TopicPartition(topic,0);
    emit("case-start",group,mode,resolution);
    try(Admin admin=Admin.create(p)) {
      admin.alterConsumerGroupOffsets(group,Map.of(tp,new OffsetAndMetadata(BASELINE,"synthetic-baseline"))).all().get(30,TimeUnit.SECONDS);
      // Bounded local-only endpoint: one broker owns every coordinator.
      OffsetFetchWireOracle.sasl=mode.equals("SASL_SSL");
      SSLSocket socket=OffsetFetchWireOracle.connect(ep); OffsetFetchWireOracle.connections.put(ep,socket);
      try {
        expect(fetch(ep,group,topic,7,true,"baseline"),0,BASELINE);
        Properties producerProperties=new Properties(); producerProperties.putAll(p);
        producerProperties.put("key.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProperties.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProperties.put("transactional.id",group+"-producer");
        producerProperties.put("transaction.timeout.ms","60000");producerProperties.put("max.block.ms","30000");
        try(KafkaProducer<byte[],byte[]> producer=new KafkaProducer<>(producerProperties)) {
          producer.initTransactions();producer.beginTransaction();
          producer.sendOffsetsToTransaction(Map.of(tp,new OffsetAndMetadata(PENDING,"synthetic-pending")),new ConsumerGroupMetadata(group));
          emit("transaction-offsets-acknowledged-open",group,mode,resolution);
          expect(fetch(ep,group,topic,5,false,"open-v5-control"),0,BASELINE);
          expect(fetch(ep,group,topic,7,false,"open-v7-control"),0,BASELINE);
          int observed88=0;
          for(int i=0;i<3;i++) {
            ObjectNode r=fetch(ep,group,topic,7,true,"pending-stable-"+i);
            if(r.path("offsets").get(0).path("errorCode").asInt()==88) observed88++;
            Thread.sleep(75);
          }
          emit(observed88==3?"pending88-verified":"pending-not88",group,mode,resolution);
          if(hold) {
            emit("awaiting-resolution-command",group,mode,resolution);
            String command=new BufferedReader(new InputStreamReader(System.in)).readLine();
            if(!resolution.equals(command)) throw new IllegalArgumentException("Expected explicit matching commit or abort command");
          }
          if(resolution.equals("commit")) producer.commitTransaction(); else producer.abortTransaction();
          emit("transaction-"+resolution+"-acknowledged",group,mode,resolution);
          long expected=resolution.equals("commit")?PENDING:BASELINE;
          boolean settled=false;
          for(int i=0;i<100;i++) {
            ObjectNode r=fetch(ep,group,topic,7,true,"post-resolution-read-retry-"+i);
            int error=r.path("offsets").get(0).path("errorCode").asInt();
            if(error==0) {expect(r,0,expected);settled=true;break;}
            if(error!=88) throw new AssertionError("Unexpected error while read-only retrying");
            Thread.sleep(50);
          }
          if(!settled) throw new AssertionError("Did not settle within bounded read-only observation window");
          expect(fetch(ep,group,topic,5,false,"settled-v5-control"),0,expected);
          if(observed88!=3) throw new AssertionError("Transaction open but genuine partition88 was not repeatedly observed; retain not88 result");
          emit("case-pass",group,mode,resolution);
        }
      } finally {OffsetFetchWireOracle.connections.remove(ep);socket.close();}
    }
  }
  public static void main(String[] args) throws Exception {
    String ca=System.getenv("KAFKA_SECURE_CA"),topic=System.getenv("KAFKA_SECURE_TOPIC");
    if(ca==null||topic==null||!topic.startsWith("secure-private-")) throw new IllegalArgumentException("Owned ephemeral secure fixture required");
    KeyStore trust=KeyStore.getInstance(KeyStore.getDefaultType());trust.load(null,null);
    try(InputStream in=Files.newInputStream(Path.of(ca))) {int i=0;for(var cert:CertificateFactory.getInstance("X.509").generateCertificates(in)) trust.setCertificateEntry("ca-"+(i++),cert);}
    TrustManagerFactory tm=TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());tm.init(trust);
    SSLContext context=SSLContext.getInstance("TLS");context.init(null,tm.getTrustManagers(),null);OffsetFetchWireOracle.factory=context.getSocketFactory();
    if(args.length==3 && args[0].equals("--hold") && List.of("SSL","SASL_SSL").contains(args[1]) && List.of("commit","abort").contains(args[2])) {
      hold=true;run(args[1],args[2],ca,topic);
    } else if(args.length==0) {
      for(String mode:List.of("SSL","SASL_SSL")) for(String resolution:List.of("commit","abort")) run(mode,resolution,ca,topic);
    } else throw new IllegalArgumentException("Usage: [--hold SSL|SASL_SSL commit|abort]");
    OffsetFetchWireOracle.emit(OffsetFetchWireOracle.JSON.createObjectNode().put("event","all-cases-pass").put("cases",hold?1:4)
      .put("officialKafkaVersion","4.0.2").put("readOnlyRetry",true).put("applicationCommitRetry",false));
  }
}
