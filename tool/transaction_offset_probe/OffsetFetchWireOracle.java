import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.security.cert.CertificateFactory;
import java.time.Instant;
import java.util.*;
import javax.net.ssl.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.*;
import org.apache.kafka.common.requests.*;

/** Single-request official Kafka 4.0.2 codec oracle. No discovery, retries, or negotiation. */
public final class OffsetFetchWireOracle {
  static final ObjectMapper JSON = new ObjectMapper();
  static final Map<String, SSLSocket> connections = new HashMap<>();
  static SSLSocketFactory factory;
  static boolean sasl;
  static int correlation;
  static final int TIMEOUT = 5000;
  record Wire(byte[] request, byte[] response, ByteBuffer body, int correlation, long start, long end, String utc, String receivedUtc) {}
  static byte[] bytes(ByteBuffer b) { byte[] a = new byte[b.remaining()]; b.get(a); return a; }
  static void emit(ObjectNode n) throws IOException { System.out.println(JSON.writeValueAsString(n)); System.out.flush(); }
  static String sha(byte[] a) throws Exception { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(a)); }
  static Wire exchange(SSLSocket socket, ApiKeys api, short version, Message data) throws Exception {
    int id = ++correlation;
    RequestHeader header = new RequestHeader(api, version, "official-wire-offset-oracle", id);
    byte[] h = bytes(MessageUtil.toByteBuffer(header.data(), header.headerVersion()));
    byte[] b = bytes(MessageUtil.toByteBuffer(data, version));
    byte[] request = ByteBuffer.allocate(4+h.length+b.length).putInt(h.length+b.length).put(h).put(b).array();
    String utc = Instant.now().toString(); long start = System.nanoTime();
    socket.getOutputStream().write(request); socket.getOutputStream().flush();
    DataInputStream input = new DataInputStream(socket.getInputStream());
    int length = input.readInt();
    if (length < 4 || length > 16*1024*1024) throw new IOException("invalid frame length");
    byte[] payload = input.readNBytes(length);
    if (payload.length != length) throw new EOFException();
    long end = System.nanoTime(); String receivedUtc = Instant.now().toString();
    byte[] response = ByteBuffer.allocate(4+length).putInt(length).put(payload).array();
    ByteBuffer body = ByteBuffer.wrap(payload);
    ResponseHeader rh = ResponseHeader.parse(body, api.responseHeaderVersion(version));
    if (rh.correlationId() != id) throw new IOException("correlation mismatch");
    return new Wire(request,response,body,id,start,end,utc,receivedUtc);
  }
  static SSLSocket connect(String endpoint) throws Exception {
    URI uri = URI.create("tls://"+endpoint);
    if (!"localhost".equals(uri.getHost()) || uri.getPort() < 1) throw new IllegalArgumentException("localhost endpoint required");
    SSLSocket socket = (SSLSocket) factory.createSocket();
    try {
      socket.setSoTimeout(TIMEOUT); socket.setTcpNoDelay(true);
      SSLParameters params = socket.getSSLParameters(); params.setEndpointIdentificationAlgorithm("HTTPS"); socket.setSSLParameters(params);
      socket.connect(new InetSocketAddress(uri.getHost(),uri.getPort()),TIMEOUT); socket.startHandshake();
      if (sasl) {
        Wire hs = exchange(socket,ApiKeys.SASL_HANDSHAKE,(short)1,new SaslHandshakeRequestData().setMechanism("PLAIN"));
        var hr = new SaslHandshakeResponseData(new ByteBufferAccessor(hs.body()),(short)1);
        if (hr.errorCode()!=0) throw new SecurityException("SASL handshake rejected");
        String user = System.getenv("KAFKA_COMBINED_USERNAME"), pass = System.getenv("KAFKA_COMBINED_PASSWORD");
        if (user==null || pass==null) throw new IllegalStateException("synthetic credentials absent");
        byte[] auth = ("\0"+user+"\0"+pass).getBytes(StandardCharsets.UTF_8);
        Wire aw = exchange(socket,ApiKeys.SASL_AUTHENTICATE,(short)1,new SaslAuthenticateRequestData().setAuthBytes(auth));
        Arrays.fill(auth,(byte)0); Arrays.fill(aw.request(),(byte)0);
        var ar = new SaslAuthenticateResponseData(new ByteBufferAccessor(aw.body()),(short)1);
        if (ar.errorCode()!=0) throw new SecurityException("SASL authentication rejected");
      }
      return socket;
    } catch(Exception e) { socket.close(); throw e; }
  }
  static ObjectNode fetch(JsonNode command) throws Exception {
    String endpoint=command.required("endpoint").asText();
    SSLSocket socket=connections.get(endpoint);
    if(socket==null) throw new IllegalStateException("connect before fetch");
    short version=(short)command.required("version").asInt();
    boolean stable=command.path("requireStable").asBoolean(false);
    if((version!=5 && version!=7) || (version==5 && stable)) throw new IllegalArgumentException("only v5(false) or v7 supported");
    List<Integer> partitions=new ArrayList<>();
    for(JsonNode p:command.required("partitions")) partitions.add(p.asInt());
    var data=new OffsetFetchRequestData().setGroupId(command.required("group").asText()).setRequireStable(stable)
      .setTopics(List.of(new OffsetFetchRequestData.OffsetFetchRequestTopic().setName(command.required("topic").asText()).setPartitionIndexes(partitions)));
    Wire w;
    try { w=exchange(socket,ApiKeys.OFFSET_FETCH,version,data); }
    catch(Exception e) { connections.remove(endpoint); socket.close(); throw e; }
    var response=new OffsetFetchResponseData(new ByteBufferAccessor(w.body()),version);
    if(w.body().hasRemaining()) throw new IOException("trailing response bytes");
    ObjectNode out=JSON.createObjectNode().put("event","fetch").put("endpoint",endpoint).put("remote",socket.getRemoteSocketAddress().toString())
      .put("version",version).put("requireStable",stable).put("correlation",w.correlation()).put("sentUtc",w.utc())
      .put("receivedUtc",w.receivedUtc()).put("requestVersion",version).put("responseVersion",version)
      .put("requestCount",1).put("autoRetry",false).put("coordinatorDiscovery",false).put("versionNegotiation",false)
      .put("startMonotonicNs",w.start()).put("endMonotonicNs",w.end()).put("roundTripNs",w.end()-w.start())
      .put("requestBytes",w.request().length).put("responseBytes",w.response().length)
      .put("requestSha256",sha(w.request())).put("responseSha256",sha(w.response()))
      .put("requestHex",HexFormat.of().formatHex(w.request())).put("responseHex",HexFormat.of().formatHex(w.response()))
      .put("groupError",Errors.forCode(response.errorCode()).name());
    out.set("decoded",OffsetFetchResponseDataJsonConverter.write(response,version));
    ArrayNode offsets=out.putArray("offsets");
    for(var t:response.topics()) for(var p:t.partitions()) offsets.addObject().put("topic",t.name()).put("partition",p.partitionIndex())
      .put("offset",p.committedOffset()).put("leaderEpoch",p.committedLeaderEpoch()).put("errorCode",p.errorCode()).put("error",Errors.forCode(p.errorCode()).name());
    return out;
  }
  static void warmup() throws Exception {
    for(short v:new short[]{5,7}) {
      var d=new OffsetFetchRequestData().setGroupId("codec-self-test").setRequireStable(false)
        .setTopics(List.of(new OffsetFetchRequestData.OffsetFetchRequestTopic().setName("test").setPartitionIndexes(List.of(0))));
      if(!d.equals(new OffsetFetchRequestData(new ByteBufferAccessor(MessageUtil.toByteBuffer(d,v)),v))) throw new AssertionError("request codec");
      var r=new OffsetFetchResponseData().setTopics(List.of(new OffsetFetchResponseData.OffsetFetchResponseTopic().setName("test")
        .setPartitions(List.of(new OffsetFetchResponseData.OffsetFetchResponsePartition().setPartitionIndex(0).setCommittedOffset(-1).setMetadata("")))));
      if(!r.equals(new OffsetFetchResponseData(new ByteBufferAccessor(MessageUtil.toByteBuffer(r,v)),v))) throw new AssertionError("response codec");
      OffsetFetchResponseDataJsonConverter.write(r,v); Errors.forCode((short)0).name();
      MessageUtil.toByteBuffer(new RequestHeader(ApiKeys.OFFSET_FETCH,v,"warmup",1).data(),ApiKeys.OFFSET_FETCH.requestHeaderVersion(v));
    }
    var stable = new OffsetFetchRequestData().setGroupId("codec-self-test").setRequireStable(true).setTopics(null);
    if (!new OffsetFetchRequestData(new ByteBufferAccessor(MessageUtil.toByteBuffer(stable,(short)7)),(short)7).requireStable()) throw new AssertionError("stable codec");
    sha(new byte[0]); JSON.writeValueAsString(JSON.readTree("{\"warmup\":true}"));
  }
  public static void main(String[] args) throws Exception {
    warmup();
    if(args.length==1 && args[0].equals("--self-test")) { emit(JSON.createObjectNode().put("event","self-test").put("ok",true)); return; }
    if(args.length!=2 || !(args[1].equals("SSL") || args[1].equals("SASL_SSL"))) throw new IllegalArgumentException("CA.pem SSL|SASL_SSL");
    sasl=args[1].equals("SASL_SSL");
    KeyStore trust=KeyStore.getInstance(KeyStore.getDefaultType()); trust.load(null,null);
    try(InputStream in=Files.newInputStream(Path.of(args[0]))) { int i=0; for(var cert:CertificateFactory.getInstance("X.509").generateCertificates(in)) trust.setCertificateEntry("ca-"+(i++),cert); }
    TrustManagerFactory tm=TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()); tm.init(trust);
    SSLContext context=SSLContext.getInstance("TLS"); context.init(null,tm.getTrustManagers(),null); factory=context.getSocketFactory();
    emit(JSON.createObjectNode().put("event","ready").put("officialKafkaVersion","4.0.2").put("securityProtocol",args[1]).put("autoRetry",false).put("versionNegotiation",false));
    try(BufferedReader input=new BufferedReader(new InputStreamReader(System.in,StandardCharsets.UTF_8))) {
      String line;
      while((line=input.readLine())!=null) {
        JsonNode command=null; ObjectNode out;
        try {
          command=JSON.readTree(line); String op=command.required("op").asText();
          if(op.equals("quit")) { emit(JSON.createObjectNode().put("event","bye")); break; }
          if(op.equals("fetch")) out=fetch(command);
          else if(op.equals("connect")) {
            String ep=command.required("endpoint").asText();
            SSLSocket old=connections.remove(ep); if(old!=null) old.close();
            long start=System.nanoTime(); SSLSocket s=connect(ep); connections.put(ep,s);
            out=JSON.createObjectNode().put("event","connected").put("endpoint",ep).put("connectNs",System.nanoTime()-start).put("tls",s.getSession().getProtocol()).put("hostnameVerification",true);
          } else if(op.equals("close")) { SSLSocket s=connections.remove(command.required("endpoint").asText()); if(s!=null)s.close(); out=JSON.createObjectNode().put("event","closed"); }
          else throw new IllegalArgumentException("unknown operation");
        } catch(Exception e) { out=JSON.createObjectNode().put("event","error").put("errorType",e.getClass().getSimpleName()); }
        if(command!=null && command.has("id")) out.set("id",command.get("id"));
        emit(out);
      }
    } finally { for(SSLSocket s:connections.values()) try{s.close();}catch(Exception ignored){} }
  }
}
