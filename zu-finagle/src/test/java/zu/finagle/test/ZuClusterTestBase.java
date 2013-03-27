package zu.finagle.test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.After;
import org.junit.Before;

import scala.runtime.BoxedUnit;
import zu.core.cluster.ZuCluster;
import zu.core.cluster.ZuClusterEventListener;
import zu.core.cluster.routing.RoutingAlgorithm;
import zu.finagle.client.ZuClientProxy;
import zu.finagle.client.ZuFinagleServiceDecorator;
import zu.finagle.client.ZuScatterGatherer;
import zu.finagle.client.ZuTransportClientProxy;
import zu.finagle.server.ZuFinagleServer;
import zu.finagle.server.ZuTransportService;

import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common.zookeeper.testing.BaseZooKeeperTest;
import com.twitter.finagle.Service;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.util.Future;
import com.twitter.util.Time;

public abstract class ZuClusterTestBase extends BaseZooKeeperTest {

  public static int port1 = 6201;
  public static ReqServiceImpl svcImpl1 = new ReqServiceImpl(new HashSet<Integer>(Arrays.asList(0, 1)));
  
  public static int port2 = 6202;
  public static ReqServiceImpl svcImpl2 = new ReqServiceImpl(new HashSet<Integer>(Arrays.asList(1, 2)));
  
  public static int port3 = 6203;
  public static ReqServiceImpl svcImpl3 = new ReqServiceImpl(new HashSet<Integer>(Arrays.asList(2, 3)));
  
  public static ZuFinagleServer buildFinagleServiceServer(int port, ReqServiceImpl svcImpl) {
    Service<byte[], byte[]> svc = new ReqService.Service(svcImpl, new TBinaryProtocol.Factory());
    return new ZuFinagleServer(port, svc);
  }
  
  public static ZuFinagleServer buildZuTransportServiceServer(int port, ReqServiceImpl svcImpl) {
    ZuTransportService zuSvc = new ZuTransportService();
    zuSvc.registerHandler(svcImpl);
    return new ZuFinagleServer(port, zuSvc.getService());
  }
  
  public static ZuScatterGatherer<Req2, Resp2> scatterGather = new ZuScatterGatherer<Req2, Resp2>(){
    @Override
    public Future<Resp2> merge(Map<Integer, Resp2> results) {
      HashSet<Integer> merged = new HashSet<Integer>();
      for (Resp2 subResult : results.values()) {
        merged.addAll(subResult.vals);
      }
      Resp2 r = new Resp2();
      r.setVals(merged);
      return Future.value(r);
    }

    @Override
    public Req2 rewrite(Req2 req, int shard) {
     return req.setNum(shard);
    }
  };
  
  private List<ZuFinagleServer> serverList = new ArrayList<ZuFinagleServer>();
  
  protected ZuCluster cluster;
  protected RoutingAlgorithm<Service<Req2, Resp2>> routingAlgorithm;
  protected ZuClientProxy<Req2, Resp2> clientProxy;
  
  public static enum ClusterType {
    Finagle,
    ZuTransport
  };
  
  protected abstract ClusterType getClusterType();
  
  @Before
  public void setup() throws Exception{
    ZooKeeperClient zkClient = createZkClient();
    cluster = new ZuCluster(zkClient, "/core/test2");
    
    ClusterType clusterType = getClusterType();
    
    List<Set<Integer>> partList = new ArrayList<Set<Integer>>();
    
    if (clusterType == ClusterType.Finagle) {
      ZuFinagleServer server = buildFinagleServiceServer(port1, svcImpl1);
      partList.add(svcImpl1.getShards());
      serverList.add(server);
      
      server = buildFinagleServiceServer(port2, svcImpl2);
      partList.add(svcImpl2.getShards());
      serverList.add(server);
      
      server = buildFinagleServiceServer(port3, svcImpl3);
      partList.add(svcImpl3.getShards());
      serverList.add(server);
      
      clientProxy = new ZuClientProxy<Req2, Resp2>() {
        
        @Override
        public Service<Req2, Resp2> wrap(final Service<ThriftClientRequest, byte[]> client) {
          final ReqService.ServiceIface svcIface =  new ReqService.ServiceToClient(client, new TBinaryProtocol.Factory());
          return new Service<Req2, Resp2>() {

            @Override
            public Future<BoxedUnit> close(Time deadline) {
              return client.close(deadline);
            }
            
            @Override
            public Future<Resp2> apply(Req2 req) {
              return svcIface.handle(req);
            }
            
          };
        }
      };
    }
    else {
      ZuFinagleServer server = buildZuTransportServiceServer(port1, svcImpl1);
      partList.add(svcImpl1.getShards());
      serverList.add(server);
      
      server = buildZuTransportServiceServer(port2, svcImpl2);
      partList.add(svcImpl2.getShards());
      serverList.add(server);
      
      server = buildZuTransportServiceServer(port3, svcImpl3);
      partList.add(svcImpl3.getShards());
      serverList.add(server);
      
      clientProxy = new ZuTransportClientProxy<>(ReqServiceImpl.SVC, ReqServiceImpl.serializer);
    }
    
    routingAlgorithm = new RoutingAlgorithm.RandomAlgorithm<>(new ZuFinagleServiceDecorator<Req2, Resp2>(clientProxy));
   
    cluster.addClusterEventListener(routingAlgorithm);
    
    final CountDownLatch latch = new CountDownLatch(4);
    final Set<Integer> shards = new HashSet<Integer>();
    
    cluster.addClusterEventListener(new ZuClusterEventListener() {
      
      @Override
      public void clusterChanged(Map<Integer, List<InetSocketAddress>> clusterView) {
        for (Integer shard : clusterView.keySet()) {
          if (!shards.contains(shard)) {
            shards.add(shard);
            latch.countDown();
          }
        }
      }
    });
    
    int c = 0;
    for (ZuFinagleServer server : serverList) {
      server.start();
      server.joinCluster(cluster, new ArrayList<Integer>(partList.get(c)));
      c++;
    }
    latch.await();
  }
  
  @After
  public void shutdown() {
    for (ZuFinagleServer s : serverList) {
      s.shutdown();
    }
  }
}
