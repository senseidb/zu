package zu.finagle.server;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import zu.core.cluster.ZuCluster;
import zu.finagle.serialize.ZuSerializer;
import zu.finagle.server.ZuTransportService.RequestHandler;

import com.google.common.base.Stopwatch;
import com.twitter.common.zookeeper.Group.JoinException;
import com.twitter.common.zookeeper.ServerSet.EndpointStatus;
import com.twitter.common.zookeeper.ServerSet.UpdateException;
import com.twitter.finagle.Service;
import com.twitter.finagle.builder.Server;
import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.thrift.ThriftServerFramedCodec;
import com.twitter.util.Duration;
import com.twitter.util.Future;

public class ZuFinagleServer{
  static final int DEFAULT_MAX_CONCURRENT_REQUESTS = 100;
  private final InetSocketAddress addr;
  private final String name;
  private Server server;
  private final Service<byte[], byte[]> svc;
  private int maxConcurrentRequests = DEFAULT_MAX_CONCURRENT_REQUESTS;
  private final Logger logger = Logger.getLogger(ZuFinagleServer.class);  
  
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
    server = ServerBuilder.safeBuild(svc,
        ServerBuilder.get()
        .codec(ThriftServerFramedCodec.get())
        .name(name).maxConcurrentRequests(maxConcurrentRequests)
        .bindTo(addr));
    sw.stop();
    logger.info(String.format("building finagle server took %s ms", sw.elapsedMillis()));
  }
  
  public void shutdown(Duration timeout){
    if (server != null) {
      Future<?> future = server.close();
      if (timeout != null) {
        future.apply(timeout);
      }
      else {
        future.apply();
      }
    }
  }
  
  public void shutdown(){
    shutdown(null);
  }

  private Map<String, List<EndpointStatus>> endpointMap = new HashMap<String, List<EndpointStatus>>();
  
  public synchronized void joinCluster(ZuCluster cluster, Set<Integer> shards) throws JoinException, InterruptedException {
    String clusterId = cluster.getClusterId();
    List<EndpointStatus> endpoints = endpointMap.get(clusterId);
    if (endpoints == null) {
      endpoints = cluster.join(addr, shards);
      endpointMap.put(clusterId, endpoints);
    }
    else {
      throw new JoinException("cluster "+clusterId+" already joined, leave first", null);
    }
  }
  
  public void leaveCluster(ZuCluster cluster) throws UpdateException{
    List<EndpointStatus> endpoints = endpointMap.remove(cluster.getClusterId());
    if (endpoints != null) {
      cluster.leave(endpoints);
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
        return svc.apply(req).apply();
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
