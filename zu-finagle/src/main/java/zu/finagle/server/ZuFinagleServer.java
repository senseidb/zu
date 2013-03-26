package zu.finagle.server;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import zu.core.cluster.ZuCluster;

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
  private final InetSocketAddress addr;
  private final String name;
  private Server server;
  private final Service<byte[], byte[]> svc;
  
  public ZuFinagleServer(int port, Service<byte[], byte[]> svc) {
    this("Zu server", port, svc);
  }
  
  public ZuFinagleServer(String name, int port, Service<byte[], byte[]> svc) {
    this.addr = new InetSocketAddress(port);
    this.name = name;
    this.server = null;
    this.svc = svc;
  }
  
  public void start() {
    server = ServerBuilder.safeBuild(svc,
        ServerBuilder.get()
        .codec(ThriftServerFramedCodec.get())
        .name(name)
        .bindTo(addr));
    
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
  
  public synchronized void joinCluster(ZuCluster cluster, List<Integer> shards) throws JoinException, InterruptedException {
    String clusterName = cluster.getClusterName();
    List<EndpointStatus> endpoints = endpointMap.get(clusterName);
    if (endpoints == null) {
      endpoints = cluster.join(addr, shards);
      endpointMap.put(clusterName, endpoints);
    }
    else {
      throw new JoinException("cluster "+clusterName+" already joined, leave first", null);
    }
  }
  
  public void leaveCluster(ZuCluster cluster) throws UpdateException{
    List<EndpointStatus> endpoints = endpointMap.remove(cluster.getClusterName());
    if (endpoints != null) {
      cluster.leave(endpoints);
    }
  }
}
