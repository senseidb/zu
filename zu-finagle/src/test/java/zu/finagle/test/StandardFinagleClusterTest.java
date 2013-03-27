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
import zu.finagle.test.ReqService.ServiceIface;
import zu.finagle.test.ZuClusterTestBase.Node;

import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common.zookeeper.testing.BaseZooKeeperTest;
import com.twitter.finagle.Service;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.builder.Server;
import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.thrift.ThriftClientFramedCodec;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.finagle.thrift.ThriftServerFramedCodec;
import com.twitter.util.Duration;
import com.twitter.util.Future;

public class StandardFinagleClusterTest extends BaseZooKeeperTest{

  private ZuCluster cluster;
  
  private List<Server> serverList = new ArrayList<Server>();
  
  private RoutingAlgorithm.RandomAlgorithm<ReqService.ServiceIface> routingAlgorithm;
  
  static final int hostConnLimit = 100;
  
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
  
  static final Duration partialResultTimeout = Duration.apply(10000, TimeUnit.MILLISECONDS);
  
  public ReqService.ServiceIface buildBrokerService(final RoutingAlgorithm.RandomAlgorithm<ReqService.ServiceIface> routingAlgorithm) {
    ReqService.ServiceIface brokerService = new ReqService.ServiceIface() {
      
      @Override
      public Future<Resp2> handle(Req2 req) {
        Set<Integer> shards = routingAlgorithm.getShards();
        Map<Integer, Future<Resp2>> futureList = new HashMap<Integer,Future<Resp2>>();
        for (Integer shard : shards) {
          ReqService.ServiceIface node = routingAlgorithm.route(null, shard);
          if (node != null) {
            Req2 rewrittenReq = ZuClusterTestBase.scatterGather.rewrite(req, shard);
            futureList.put(shard, node.handle(rewrittenReq));
          }
        }
        
        Map<Integer, Resp2> resList = new HashMap<Integer,Resp2>();
        
        for (Entry<Integer, Future<Resp2>> entry : futureList.entrySet()) {
          Resp2 result = entry.getValue().apply(partialResultTimeout);
          resList.put(entry.getKey(), result);
        }
        
        return ZuClusterTestBase.scatterGather.merge(resList);
      }
    };
    
    return brokerService;
  }
  
  @Before
  public void setup() throws Exception{
    ZooKeeperClient zkClient = createZkClient();
    cluster = new ZuCluster(zkClient, "/core/test3");
    

    routingAlgorithm = new RoutingAlgorithm.RandomAlgorithm<ReqService.ServiceIface>(clientServiceBuilder);
    
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
    });
    
    // start the servers and join the cluster
    
    for (Node node : ZuClusterTestBase.nodes) {
      
      ReqServiceImpl svcImpl = node.svc;
      InetSocketAddress addr = new InetSocketAddress(node.port);
      
      // build a standard finagle server from svc
      Server server = ServerBuilder.safeBuild(
          new ReqService.Service(svcImpl, new TBinaryProtocol.Factory()),
          ServerBuilder.get()
                  .name("TestServer:"+node.port)
                  .codec(ThriftServerFramedCodec.get())
                  .bindTo(addr));
      
      cluster.join(addr, svcImpl.getShards());
      serverList.add(server);
    }
    
    latch.await();
  }
  
  @Test
  public void testBrokerService() throws Exception {
    ReqService.ServiceIface brokerSvc = buildBrokerService(routingAlgorithm);
    Future<Resp2> future  = brokerSvc.handle(new Req2());
    Resp2 merged = future.apply();
    TestCase.assertEquals(new HashSet<Integer>(Arrays.asList(0,1,2,3)), merged.getVals());
  }
  
  @Test
  public void testBrokerServiceAsAServer() throws Exception {
    int brokerPort = 6667;
    InetSocketAddress brokerAddr = new InetSocketAddress(brokerPort);
    Server broker = null;
    
    try {
      
      ReqService.ServiceIface brokerSvc = buildBrokerService(routingAlgorithm);
      
      // start broker as a service
      broker = ServerBuilder.safeBuild(
          new ReqService.Service(brokerSvc, new TBinaryProtocol.Factory()),
          ServerBuilder.get()
                  .name("TestBroker:"+ brokerPort)
                  .codec(ThriftServerFramedCodec.get())
                  .bindTo(brokerAddr));
      
      
      ReqService.ServiceIface brokerClient = clientServiceBuilder.decorate(brokerAddr);
      
      Future<Resp2> future  = brokerClient.handle(new Req2());
      Resp2 merged = future.apply();
      TestCase.assertEquals(new HashSet<Integer>(Arrays.asList(0,1,2,3)), merged.getVals());
      
    }
    finally {
      if (broker != null) {
        broker.close();
      }
    }
  }
  

  @After
  public void shutdown() {
    for (Server s : serverList) {
      s.close().apply();
    }
  }
}
