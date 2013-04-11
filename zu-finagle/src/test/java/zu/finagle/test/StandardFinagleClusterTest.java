package zu.finagle.test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import zu.core.cluster.ZuCluster;
import zu.core.cluster.ZuClusterEventListener;
import zu.core.cluster.routing.InetSocketAddressDecorator;
import zu.core.cluster.routing.RoutingAlgorithm;
import zu.core.cluster.routing.ZuScatterGatherer;
import zu.finagle.server.ZuFinagleServer;
import zu.finagle.test.ReqService.ServiceIface;
import zu.finagle.test.ZuClusterTestBase.Node;

import com.twitter.common.zookeeper.ServerSet.UpdateException;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common.zookeeper.testing.BaseZooKeeperTest;
import com.twitter.finagle.Service;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.thrift.ThriftClientFramedCodec;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.util.Duration;
import com.twitter.util.Future;

public class StandardFinagleClusterTest extends BaseZooKeeperTest{

  // cluster
  private ZuCluster cluster;
  
  // a pool of servers
  private List<ZuFinagleServer> serverList = new ArrayList<ZuFinagleServer>();
  
  private RoutingAlgorithm.RandomAlgorithm<ReqService.ServiceIface> routingAlgorithm;
  
  static final int hostConnLimit = 100;
  
  // this decorates an InetSocketAddress into a client interface to a ReqService.ServiceIface service
  private InetSocketAddressDecorator<ReqService.ServiceIface> clientServiceBuilder = new InetSocketAddressDecorator<ReqService.ServiceIface>() {

    @Override
    public ServiceIface decorate(InetSocketAddress addr) {
      Service<ThriftClientRequest, byte[]> client = ClientBuilder.safeBuild(ClientBuilder.get()
          .hosts(addr)
          .codec(ThriftClientFramedCodec.get())
          .hostConnectionLimit(hostConnLimit));
      
      return new ReqService.ServiceToClient(client, new TBinaryProtocol.Factory());
    }

    @Override
    public void cleanup(Set<ServiceIface> toBeClosed) {

    }
  };
  
  // timeout for partial results
  static final Duration partialResultTimeout = Duration.apply(10000, TimeUnit.MILLISECONDS);
  
  
  /** implementation for a broker service
   * @param routingAlgorithm al routing algorithm
   * @param scatterGather scatter gather implementation for the broker
   * @return broker service implementation
   */
  public ReqService.ServiceIface buildBrokerService(final RoutingAlgorithm.RandomAlgorithm<ReqService.ServiceIface> routingAlgorithm,
      final ZuScatterGatherer<Req2, Resp2> scatterGather) {
    
    ReqService.ServiceIface brokerService = new ReqService.ServiceIface() {
      
      @Override
      public Future<Resp2> handle(Req2 req) {
        // get all the shards in the cluster
        Set<Integer> shards = routingAlgorithm.getShards();
        Map<Integer, Future<Resp2>> futureList = new HashMap<Integer,Future<Resp2>>();
        
        // for eah shard
        for (Integer shard : shards) {
          // get a service from the routing algorithm
          ReqService.ServiceIface node = routingAlgorithm.route(null, shard);
          if (node != null) {
            // rewrite the request according to this shard
            Req2 rewrittenReq = scatterGather.rewrite(req, shard);
            
            // keep a future of it
            futureList.put(shard, node.handle(rewrittenReq));
          }
        }
        
        Map<Integer, Resp2> resList = new HashMap<Integer,Resp2>();
        
        // gather all the results from each shard
        for (Entry<Integer, Future<Resp2>> entry : futureList.entrySet()) {
          Resp2 result = entry.getValue().apply(partialResultTimeout);
          resList.put(entry.getKey(), result);
        }
        
        // return the merged result
        return Future.value(scatterGather.merge(resList));
      }
    };
    
    // returns the broker implementation
    return brokerService;
  }
  
  @Before
  public void setup() throws Exception{
    ZooKeeperClient zkClient = createZkClient();
    
    // create a new cluster on a given path
    cluster = new ZuCluster(zkClient, "/core/test3");
    
    // instantiate a random routing algorithm
    routingAlgorithm = new RoutingAlgorithm.RandomAlgorithm<ReqService.ServiceIface>(clientServiceBuilder);
    
    // connect the routing algorithm to the cluster
    cluster.addClusterEventListener(routingAlgorithm);
      
    
// make sure all servers are started and joined the cluster
    
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

      @Override
      public void nodesRemoved(Set<InetSocketAddress> removedNodes) {
        
      }
    });
    
    // start the servers and join the cluster
    
    for (Node node : ZuClusterTestBase.nodes) {
      
      ReqServiceImpl svcImpl = node.svc;
      
      // build a zu finagle server from svc
      ZuFinagleServer server = new ZuFinagleServer("TestServer:"+node.port, node.port, 
          new ReqService.Service(svcImpl, new TBinaryProtocol.Factory()));
     
      server.start();
      server.joinCluster(cluster, svcImpl.getShards());
      
      serverList.add(server);
    }
    
    latch.await();
  }
  
  @Test
  /**
   * Tests the broker logic as a service
   * @throws Exception
   */
  public void testBrokerService() throws Exception {
    ReqService.ServiceIface brokerSvc = buildBrokerService(routingAlgorithm, ZuClusterTestBase.scatterGather);
    Future<Resp2> future  = brokerSvc.handle(new Req2());
    Resp2 merged = future.apply();
    TestCase.assertEquals(new HashSet<Integer>(Arrays.asList(0,1,2,3)), merged.getVals());
  }
  
  @Test
  /**
   * Tests the broker logic as a server
   * @throws Exception
   */
  public void testBrokerServiceAsAServer() throws Exception {
    int brokerPort = 6667;
    InetSocketAddress brokerAddr = new InetSocketAddress(brokerPort);
    ZuFinagleServer broker = null;
    
    try {
      
      ReqService.ServiceIface brokerSvc = buildBrokerService(routingAlgorithm, ZuClusterTestBase.scatterGather);
      
      // start broker as a zu finagle server
      broker = new ZuFinagleServer("TestBroker:"+ brokerPort, brokerPort, new ReqService.Service(brokerSvc, new TBinaryProtocol.Factory())); 
          
      broker.start();
      
      ReqService.ServiceIface brokerClient = clientServiceBuilder.decorate(brokerAddr);
      
      Future<Resp2> future  = brokerClient.handle(new Req2());
      Resp2 merged = future.apply();
      TestCase.assertEquals(new HashSet<Integer>(Arrays.asList(0,1,2,3)), merged.getVals());
      
    }
    finally {
      if (broker != null) {
        broker.shutdown();
      }
    }
  }
  

  @After
  public void shutdown() {
    for (ZuFinagleServer s : serverList) {
      try{
        s.leaveCluster(cluster);
      } catch (UpdateException e) {
        TestCase.fail(e.getMessage());
      }
      finally{
        s.shutdown();
      }
    }
  }
}
