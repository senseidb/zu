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

import org.junit.Test;

import zu.core.cluster.ZuCluster;
import zu.core.cluster.ZuClusterEventListener;
import zu.core.cluster.routing.RoutingAlgorithm;
import zu.finagle.client.ZuClientFinagleServiceBuilder;
import zu.finagle.client.ZuFinagleServiceDecorator;
import zu.finagle.client.ZuScatterGatherer;
import zu.finagle.serialize.JOSSSerializer;
import zu.finagle.serialize.ThriftSerializer;
import zu.finagle.serialize.ZuSerializer;
import zu.finagle.server.ZuFinagleServer;

import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common.zookeeper.testing.BaseZooKeeperTest;
import com.twitter.finagle.Service;
import com.twitter.util.Future;

public class ZuFinagleTest extends BaseZooKeeperTest{

  
  private static class TestHandler implements ZuFinagleServer.RequestHandler<Req, Resp>{
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
  
  private static class TestClusterHandler implements ZuFinagleServer.RequestHandler<Integer, HashSet<Integer>>{
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
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void testBasic() {
    int port = 6100;
    ZuFinagleServer server = new ZuFinagleServer(port);
    
    server.registerHandler(new TestHandler());
    
    server.start();
    Service<Req, Resp> svc = null;
    try {
      ZuClientFinagleServiceBuilder<Req, Resp> builder = new ZuClientFinagleServiceBuilder<Req, Resp>();
      svc = builder.name(TestHandler.SVC).serializer(TestHandler.serializer).host(new InetSocketAddress(port)).build();
      
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
  @SuppressWarnings("unchecked")
  public void testCluster() throws Exception{
    
    List<ZuFinagleServer> serverList = new ArrayList<ZuFinagleServer>();
    
    ZooKeeperClient zkClient = createZkClient();
    ZuCluster mockCluster = new ZuCluster(zkClient, "/core/test2");
    // start 3 servers
    
    RoutingAlgorithm<Service<Integer, HashSet<Integer>>> routingAlgorithm = 
        new RoutingAlgorithm.RandomAlgorithm<>(new ZuFinagleServiceDecorator<Integer, HashSet<Integer>>(TestClusterHandler.SVC, TestClusterHandler.serializer));
        
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
    ZuFinagleServer server = new ZuFinagleServer(port);
    List<Integer> shards = Arrays.asList(0,1);
    server.registerHandler(new TestClusterHandler(new HashSet<Integer>(shards)));
    server.joinCluster(mockCluster, shards);
    serverList.add(server);
    
    port = 6102;
    server = new ZuFinagleServer(port);
    shards = Arrays.asList(1,2);
    server.registerHandler(new TestClusterHandler(new HashSet<Integer>(shards)));
    server.joinCluster(mockCluster, shards);
    serverList.add(server);
    
    port = 6103;
    server = new ZuFinagleServer(port);
    shards = Arrays.asList(2,3);
    server.registerHandler(new TestClusterHandler(new HashSet<Integer>(shards)));
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
      Service<Integer, HashSet<Integer>> svc = builder.name(TestClusterHandler.SVC).serializer(TestClusterHandler.serializer).scatterGather(scatterGather)
          .routingAlgorithm(routingAlgorithm).build();
      
      Future<HashSet<Integer>> future = svc.apply(1);
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
