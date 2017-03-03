package zu.finagle.server;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.twitter.common.zookeeper.Group.JoinException;
import com.twitter.finagle.ServerCodecConfig;
import com.twitter.finagle.Service;
import com.twitter.finagle.builder.Server;
import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.thrift.ThriftServerFramedCodec;
import com.twitter.util.Duration;
import com.twitter.util.Future;

import zu.core.cluster.ClusterRef;
import zu.core.cluster.ZuCluster;
import zu.finagle.serialize.ZuSerializer;
import zu.finagle.server.ZuTransportService.RequestHandler;

public class ZuFinagleServer{
  static final int DEFAULT_MAX_CONCURRENT_REQUESTS = 100;
  private final InetSocketAddress addr;
  private final String name;
  private Server server;
  private final Service<byte[], byte[]> svc;
  private int maxConcurrentRequests = DEFAULT_MAX_CONCURRENT_REQUESTS;
  private static final Logger logger = LoggerFactory.getLogger(ZuFinagleServer.class);  
  
  public int getMaxConcurrentRequests() {
    return maxConcurrentRequests;
  }

  public void setMaxConcurrentRequests(int maxConcurrentRequests) {
    this.maxConcurrentRequests = maxConcurrentRequests;
  }

  public ZuFinagleServer(int port, Service<byte[], byte[]> svc) {
    this("Zu server", port, svc);
  }
  
  public ZuFinagleServer(String name, int port, Service<byte[], byte[]> svc) {
    this.addr = new InetSocketAddress(port);
    this.name = name;
    this.server = null;
    this.svc = svc;
  }

  public ZuFinagleServer(String name, InetSocketAddress addr, Service<byte[], byte[]> svc) {
    this.addr = addr;
    this.name = name;
    this.server = null;
    this.svc = svc;
  }
  
  public void start() {
   Stopwatch sw = new Stopwatch();
   sw.start();
   
   ServerCodecConfig codeConfig = new ServerCodecConfig(name, addr);
   ThriftServerFramedCodec codec = new ThriftServerFramedCodec(codeConfig, new TCompactProtocol.Factory());
   
    server = ServerBuilder.safeBuild(svc,
        ServerBuilder.get()
        .codec(codec)
        .name(name).maxConcurrentRequests(maxConcurrentRequests)
        .bindTo(addr));
    sw.stop();
    logger.info(String.format("building finagle server took %s ms", sw.elapsed(TimeUnit.MILLISECONDS)));
  }
  
  public void shutdown(Duration timeout){
    if (server != null) {
      Future<?> future = server.close();
      try {
        if (timeout != null) {
          future.toJavaFuture().get(timeout.inMillis(), TimeUnit.MILLISECONDS);
        }
        else {
          future.toJavaFuture().get();
        }
      } catch(Exception e) {
        logger.error("problem shutting down", e);
      }
    }
  }
  
  public void shutdown(){
    shutdown(null);
  }

  private Map<String, ClusterRef> clusterRefMap = new HashMap<>();
  
  public synchronized void joinCluster(ZuCluster cluster, Set<Integer> shards) throws Exception {
    String clusterId = cluster.id();
    ClusterRef clusterRef = clusterRefMap.get(clusterId);
    if (clusterRef == null) {
      clusterRef = cluster.join(addr, shards);
      clusterRefMap.put(clusterId, clusterRef);
    }
    else {
      throw new JoinException("cluster "+clusterId+" already joined, leave first", null);
    }
  }
  
  public void leaveCluster(ZuCluster cluster) throws Exception{
    ClusterRef clusterRef = clusterRefMap.remove(cluster.id());
    if (clusterRef != null) {
      clusterRef.leave();
    }
  }
  
  public static <Req, Res> ZuFinagleServer buildBroker(final String name, int port, final Service<Req, Res> svc, final ZuSerializer<Req, Res> serializer) {
    ZuTransportService transportSvc = new ZuTransportService();
    transportSvc.registerHandler(new RequestHandler<Req, Res>() {

      @Override
      public String getName() {
        return name;
      }

      @Override
      public Res handleRequest(Req req) {
        try {
          return svc.apply(req).toJavaFuture().get();
        } catch (Exception e) {
          logger.error("error: ", e);
          return null;
        }
      }

      @Override
      public ZuSerializer<Req, Res> getSerializer() {
        return serializer;
      }
    });
    ZuFinagleServer broker = new ZuFinagleServer(port, transportSvc.getService());
    return broker;
  }
}
