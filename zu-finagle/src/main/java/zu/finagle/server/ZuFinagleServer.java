package zu.finagle.server;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.protocol.TBinaryProtocol;

import zu.core.cluster.ZuCluster;
import zu.finagle.rpc.ZuTransport;
import zu.finagle.rpc.ZuThriftService;
import zu.finagle.serialize.ZuSerializer;

import com.twitter.common.zookeeper.Group.JoinException;
import com.twitter.common.zookeeper.ServerSet.EndpointStatus;
import com.twitter.common.zookeeper.ServerSet.UpdateException;
import com.twitter.finagle.builder.Server;
import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.thrift.ThriftServerFramedCodec;
import com.twitter.util.Duration;
import com.twitter.util.Future;

public class ZuFinagleServer implements ZuThriftService.ServiceIface{

  public interface RequestHandler<Req, Res> {
    String getName();
    Res handleRequest(Req req);
    ZuSerializer<Req, Res> getSerializer(); 
  }

  private final InetSocketAddress addr;
  private final String name;
  private final Map<String,RequestHandler<?,?>> reqHandlerMap;
  private Server server;
  
  public ZuFinagleServer(int port) {
    this("Zu server", port);
  }
  
  public ZuFinagleServer(String name, int port) {
    this.addr = new InetSocketAddress(port);
    this.name = name;
    this.reqHandlerMap = new HashMap<String, RequestHandler<?,?>>();
    this.server = null;
  }
  
  public <Req, Res> void registerHandler(RequestHandler<Req, Res> reqHandler) {
    reqHandlerMap.put(reqHandler.getName(), reqHandler);
  }
  
  public void start() {
    server = ServerBuilder.safeBuild(new ZuThriftService.Service(this, new TBinaryProtocol.Factory()),
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

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Future<ZuTransport> send(ZuTransport req) {
    String name = req.getName();
    RequestHandler handler = reqHandlerMap.get(name);
    if (handler == null) {
      return Future.exception(new IllegalArgumentException("handler "+name+" is not registered"));
    }
    try {
      ZuSerializer serializer = handler.getSerializer();
      Object reqObj = serializer.deserializeRequest(req.data);
      Object res = handler.handleRequest(reqObj);
      ByteBuffer bytes = serializer.serializeResponse(res);
    
      ZuTransport resp = new ZuTransport();
      resp.setName(name);
      resp.setData(bytes);
      return Future.value(resp);
    }
    catch(Throwable th) {
      return Future.exception(th);
    }
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
  
  public static void main(String[] args) throws Exception{
    ZuFinagleServer server = new ZuFinagleServer("", 8123);
    server.start();
  }
}
