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

import zu.core.cluster.ZuCluster;
import zu.core.cluster.ZuClusterEventListener;
import zu.finagle.serialize.JOSSSerializer;
import zu.finagle.serialize.ZuSerializer;
import zu.finagle.server.ZuFinagleServer;
import zu.finagle.server.ZuTransportService;

import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common.zookeeper.testing.BaseZooKeeperTest;
import com.twitter.finagle.Service;
import com.twitter.util.Future;

public abstract class ZuFinagleClusterTest extends BaseZooKeeperTest {

  static class TestClusterHandler implements ZuTransportService.RequestHandler<Integer, HashSet<Integer>>, ReqService.ServiceIface{
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
      return new ReqService.Service(this, new TBinaryProtocol.Factory());
    }
  }
  
  private List<ZuFinagleServer> serverList = new ArrayList<ZuFinagleServer>();
  private List<List<Integer>> partList = new ArrayList<List<Integer>>();
  protected ZuCluster cluster;
  
  public void startServers() throws Exception{
    final CountDownLatch latch = new CountDownLatch(4);
    final Set<Integer> parts = new HashSet<Integer>();
    
    cluster.addClusterEventListener(new ZuClusterEventListener() {
      
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
    
    int c = 0;
    for (ZuFinagleServer s : serverList) {
      s.start();
      s.joinCluster(cluster, partList.get(c));
      c++;
    }
  
    latch.await();
  }
  
  protected abstract ZuFinagleServer buildServer(TestClusterHandler handler, int port);
  
  @Before
  public void init() throws Exception{
    ZooKeeperClient zkClient = createZkClient();
    cluster = new ZuCluster(zkClient, "/core/test2");
    
    
    int port = 6201;

    List<Integer> shards = Arrays.asList(0,1);
    TestClusterHandler zuSvc = new TestClusterHandler(new HashSet<Integer>(shards));
    ZuFinagleServer server = buildServer(zuSvc, port);
    serverList.add(server);
    partList.add(shards);
    
    port = 6202;
    
    shards = Arrays.asList(1,2);
    zuSvc = new TestClusterHandler(new HashSet<Integer>(shards));
    server = buildServer(zuSvc, port);
    
    serverList.add(server);
    partList.add(shards);
    
    port = 6203;
    shards = Arrays.asList(2,3);
    zuSvc = new TestClusterHandler(new HashSet<Integer>(shards));
    server = buildServer(zuSvc, port);
    
    serverList.add(server);
    partList.add(shards);
    
  }
  
  @After
  public void shutdown() {
    for (ZuFinagleServer s : serverList) {
      s.shutdown();
    }
  }
}
