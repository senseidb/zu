package zu.finagle.test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import junit.framework.TestCase;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Test;

import scala.runtime.BoxedUnit;
import zu.core.cluster.ZuCluster;
import zu.core.cluster.ZuClusterEventListener;
import zu.core.cluster.routing.RoutingAlgorithm;
import zu.finagle.client.ZuClientFinagleServiceBuilder;
import zu.finagle.client.ZuClientProxy;
import zu.finagle.client.ZuFinagleServiceDecorator;
import zu.finagle.client.ZuScatterGatherer;
import zu.finagle.client.ZuTransportClientProxy;
import zu.finagle.serialize.JOSSSerializer;
import zu.finagle.serialize.ThriftSerializer;
import zu.finagle.serialize.ZuSerializer;
import zu.finagle.server.ZuFinagleServer;
import zu.finagle.server.ZuTransportService;

import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common.zookeeper.testing.BaseZooKeeperTest;
import com.twitter.finagle.Service;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.util.Future;
import com.twitter.util.Time;

public class ZuFinagleTest extends BaseZooKeeperTest{

  
  private static class TestHandler implements ZuTransportService.RequestHandler<Req, Resp>{
    static final String SVC = "strlen";

    @SuppressWarnings("rawtypes")
    static final ZuSerializer serializer = new ThriftSerializer<Req, Resp>(Req.class, Resp.class);
    @Override
    public String getName() {
      return SVC;
    }

    @Override
    public Resp handleRequest(Req req) {
      int len = (req.s == null ) ? 0 : req.s.length();
      Resp testResp = new Resp();
      testResp.setLen(len);
      return testResp;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ZuSerializer<Req, Resp> getSerializer() {
      return serializer;
    }
    
  }
  
  private static class TestClusterHandler implements ZuTransportService.RequestHandler<Integer, HashSet<Integer>>, TestService.ServiceIface{
    static final String SVC = "cluster";

    @SuppressWarnings("rawtypes")
    static final ZuSerializer serializer = new JOSSSerializer();
    
    private final HashSet<Integer> shards;
    TestClusterHandler(HashSet<Integer> shards){
      this.shards = shards;
    }
    
    @Override
    public String getName() {
      return SVC;
    }

    @Override
    public HashSet<Integer> handleRequest(Integer req) {
      if (shards.contains(req)){
        return new HashSet<Integer>(Arrays.asList(req));
      }
      return new HashSet<Integer>();
    }

    @Override
    @SuppressWarnings("unchecked")
    public ZuSerializer<Integer, HashSet<Integer>> getSerializer() {
      return serializer;
    }

    @Override
    public Future<Resp2> handle(Req2 req) {
      Resp2 resp = new Resp2();
      resp.setVals(new HashSet<Integer>());
      if (shards.contains(req.getNum())){
        resp.vals.add(req.getNum());  
      }
      return Future.value(resp);
    }
    
    public Service<byte[], byte[]> getService() {
      return new TestService.Service(this, new TBinaryProtocol.Factory());
    }
  }
  
  
  
  @Test
  @SuppressWarnings("unchecked")
  public void testBasic() {
    int port = 6100;
    
    ZuTransportService zuSvc = new ZuTransportService();
    zuSvc.registerHandler(new TestHandler());
    
    ZuFinagleServer server = new ZuFinagleServer(port, zuSvc.getService());
    
    
    
    server.start();
    Service<Req, Resp> svc = null;
    try {
      ZuClientFinagleServiceBuilder<Req, Resp> builder = new ZuClientFinagleServiceBuilder<Req, Resp>();
      ZuTransportClientProxy<Req, Resp> clientProxy = new ZuTransportClientProxy<>(TestHandler.SVC, TestHandler.serializer);
      svc = builder.clientProxy(clientProxy).host(new InetSocketAddress(port)).build();
      
      String s = "zu finagle test string";
      Req req = new Req();
      req.setS(s);
      Future<Resp> lenFuture = svc.apply(req);
      
      Resp resp = lenFuture.apply();
      
      TestCase.assertEquals(s.length(), resp.getLen());
      
      req = new Req();
      lenFuture = svc.apply(req);
      
      resp = lenFuture.apply();
      
      TestCase.assertEquals(0, resp.getLen());
    }
    finally {
      svc.close().apply();
      server.shutdown();
    }
  }
  
  @Test
  public void testCluster1() throws Exception{
    List<ZuFinagleServer> serverList = new ArrayList<ZuFinagleServer>();
    
    ZooKeeperClient zkClient = createZkClient();
    ZuCluster mockCluster = new ZuCluster(zkClient, "/core/test2");
    // start 3 servers
    
    ZuClientProxy<Req2, Resp2> clientProxy = new ZuClientProxy<Req2, Resp2>() {
      
      @Override
      public Service<Req2, Resp2> wrap(final Service<ThriftClientRequest, byte[]> client) {
        final TestService.ServiceIface svcIface =  new TestService.ServiceToClient(client, new TBinaryProtocol.Factory());
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
    
    RoutingAlgorithm<Service<Req2, Resp2>> routingAlgorithm = 
        new RoutingAlgorithm.RandomAlgorithm<>(new ZuFinagleServiceDecorator<Req2, Resp2>(clientProxy));
        
    mockCluster.addClusterEventListener(routingAlgorithm);
    
    final CountDownLatch latch = new CountDownLatch(4);
    final Set<Integer> parts = new HashSet<Integer>();
    
    mockCluster.addClusterEventListener(new ZuClusterEventListener() {
      
      @Override
      public void clusterChanged(Map<Integer, List<InetSocketAddress>> clusterView) {
        for (Integer part : clusterView.keySet()) {
          if (!parts.contains(part)) {
            parts.add(part);
            latch.countDown();
          }
        }
      }
    });
    
    int port = 6201;

    List<Integer> shards = Arrays.asList(0,1);
    TestClusterHandler zuSvc = new TestClusterHandler(new HashSet<Integer>(shards));
    ZuFinagleServer server = new ZuFinagleServer(port, zuSvc.getService());
    
    
    server.joinCluster(mockCluster, shards);
    serverList.add(server);
    
    port = 6202;
    
    shards = Arrays.asList(1,2);
    zuSvc = new TestClusterHandler(new HashSet<Integer>(shards));
    server = new ZuFinagleServer(port, zuSvc.getService());
    
    server.joinCluster(mockCluster, shards);
    serverList.add(server);
    
    port = 6203;
    shards = Arrays.asList(2,3);
    zuSvc = new TestClusterHandler(new HashSet<Integer>(shards));
    server = new ZuFinagleServer(port, zuSvc.getService());
    
    server.joinCluster(mockCluster, shards);
    serverList.add(server);
    
    latch.await();
    
    for (ZuFinagleServer s : serverList) {
      s.start();
    }
    
    ZuScatterGatherer<Req2, Resp2> scatterGather = new ZuScatterGatherer<Req2, Resp2>(){
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
    
    try {
      ZuClientFinagleServiceBuilder<Req2, Resp2> builder = new ZuClientFinagleServiceBuilder<Req2, Resp2>();
      Service<Req2, Resp2> svc = builder.scatterGather(scatterGather).routingAlgorithm(routingAlgorithm).build();
      
      Future<Resp2> future = svc.apply(new Req2());
      Resp2 merged = future.apply();
      TestCase.assertEquals(new HashSet<Integer>(Arrays.asList(0,1,2,3)), merged.getVals());
    }
    finally {
      for (ZuFinagleServer s : serverList) {
        s.shutdown();
      }
    }
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void testCluster2() throws Exception{
    
    List<ZuFinagleServer> serverList = new ArrayList<ZuFinagleServer>();
    
    ZooKeeperClient zkClient = createZkClient();
    ZuCluster mockCluster = new ZuCluster(zkClient, "/core/test3");
    // start 3 servers
    
    ZuTransportClientProxy<Integer, HashSet<Integer>> clientProxy = new ZuTransportClientProxy<>(TestClusterHandler.SVC, TestClusterHandler.serializer);
    
    RoutingAlgorithm<Service<Integer, HashSet<Integer>>> routingAlgorithm = 
        new RoutingAlgorithm.RandomAlgorithm<>(new ZuFinagleServiceDecorator<Integer, HashSet<Integer>>(clientProxy));
        
    mockCluster.addClusterEventListener(routingAlgorithm);
    
    final CountDownLatch latch = new CountDownLatch(4);
    final Set<Integer> parts = new HashSet<Integer>();
    
    mockCluster.addClusterEventListener(new ZuClusterEventListener() {
      
      @Override
      public void clusterChanged(Map<Integer, List<InetSocketAddress>> clusterView) {
        for (Integer part : clusterView.keySet()) {
          if (!parts.contains(part)) {
            parts.add(part);
            latch.countDown();
          }
        }
      }
    });
    
    int port = 6101;

    List<Integer> shards = Arrays.asList(0,1);
    ZuTransportService zuSvc = new ZuTransportService();
    zuSvc.registerHandler(new TestClusterHandler(new HashSet<Integer>(shards)));
    ZuFinagleServer server = new ZuFinagleServer(port, zuSvc.getService());
    
    
    server.joinCluster(mockCluster, shards);
    serverList.add(server);
    
    port = 6102;
    
    shards = Arrays.asList(1,2);
    zuSvc = new ZuTransportService();
    zuSvc.registerHandler(new TestClusterHandler(new HashSet<Integer>(shards)));
    server = new ZuFinagleServer(port, zuSvc.getService());
    
    server.joinCluster(mockCluster, shards);
    serverList.add(server);
    
    port = 6103;
    shards = Arrays.asList(2,3);
    zuSvc = new ZuTransportService();
    zuSvc.registerHandler(new TestClusterHandler(new HashSet<Integer>(shards)));
    server = new ZuFinagleServer(port, zuSvc.getService());
    
    server.joinCluster(mockCluster, shards);
    serverList.add(server);
    
    latch.await();
    
    for (ZuFinagleServer s : serverList) {
      s.start();
    }
    
    ZuScatterGatherer<Integer, HashSet<Integer>> scatterGather = new ZuScatterGatherer<Integer, HashSet<Integer>>(){
      @Override
      public Future<HashSet<Integer>> merge(Map<Integer, HashSet<Integer>> results) {
        HashSet<Integer> merged = new HashSet<Integer>();
        for (HashSet<Integer> subResult : results.values()) {
          merged.addAll(subResult);
        }
        return Future.value(merged);
      }

      @Override
      public Integer rewrite(Integer req, int shard) {
       return shard;
      }
    };
    
    try {
      ZuClientFinagleServiceBuilder<Integer, HashSet<Integer>> builder = new ZuClientFinagleServiceBuilder<Integer, HashSet<Integer>>();
      Service<Integer, HashSet<Integer>> svc = builder.scatterGather(scatterGather).routingAlgorithm(routingAlgorithm).build();
      
      Future<HashSet<Integer>> future = svc.apply(0);
      HashSet<Integer> merged = future.apply();
      TestCase.assertEquals(new HashSet<Integer>(Arrays.asList(0,1,2,3)), merged);
    }
    finally {
      for (ZuFinagleServer s : serverList) {
        s.shutdown();
      }
    }
  }
}
