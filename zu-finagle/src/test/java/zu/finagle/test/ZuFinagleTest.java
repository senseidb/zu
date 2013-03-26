package zu.finagle.test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;

import zu.finagle.ZuClientFinagleServiceFactory;
import zu.finagle.serialize.JOSSSerializer;
import zu.finagle.serialize.ThriftSerializer;
import zu.finagle.serialize.ZuSerializer;
import zu.finagle.server.ZuFinagleServer;

import com.twitter.common.zookeeper.testing.BaseZooKeeperTest;
import com.twitter.finagle.Service;
import com.twitter.util.Future;

public class ZuFinagleTest extends BaseZooKeeperTest{

  
  private static class TestHandler implements ZuFinagleServer.RequestHandler<TestReq, TestResp>{
    static final String SVC = "strlen";

    @SuppressWarnings("rawtypes")
    static final ZuSerializer serializer = new ThriftSerializer<TestReq, TestResp>(TestReq.class, TestResp.class);
    @Override
    public String getName() {
      return SVC;
    }

    @Override
    public TestResp handleRequest(TestReq req) {
      int len = (req.s == null ) ? 0 : req.s.length();
      TestResp testResp = new TestResp();
      testResp.setLen(len);
      return testResp;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ZuSerializer<TestReq, TestResp> getSerializer() {
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
        return shards;
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
    ZuFinagleServer server = new ZuFinagleServer("test", port);
    
    server.registerHandler(new TestHandler());
    
    server.start();
    
    try {
      ZuClientFinagleServiceFactory clientFactory = new ZuClientFinagleServiceFactory(new InetSocketAddress(port), 1000, 5);
      clientFactory.registerSerializer(TestHandler.SVC, TestHandler.serializer);
      Service<TestReq, TestResp> svc = clientFactory.getService(TestHandler.SVC);
      
      String s = "zu finagle test string";
      TestReq req = new TestReq();
      req.setS(s);
      Future<TestResp> lenFuture = svc.apply(req);
      
      TestResp resp = lenFuture.apply();
      
      TestCase.assertEquals(s.length(), resp.getLen());
      
      req = new TestReq();
      lenFuture = svc.apply(req);
      
      resp = lenFuture.apply();
      
      TestCase.assertEquals(0, resp.getLen());
    }
    finally {
      server.shutdown();
    }
  }
  
  @Test
  public void testCluster(){
    
    List<ZuFinagleServer> serverList = new ArrayList<ZuFinagleServer>();
    
    int port = 6100;
    ZuFinagleServer server = new ZuFinagleServer("test", port);
    server.registerHandler(new TestHandler());
    serverList.add(server);
    
    server.start();
  }
}
