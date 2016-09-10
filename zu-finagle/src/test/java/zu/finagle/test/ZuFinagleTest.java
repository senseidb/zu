package zu.finagle.test;

import java.net.InetSocketAddress;

import junit.framework.TestCase;

import org.junit.Test;

import zu.finagle.client.ZuClientFinagleServiceBuilder;
import zu.finagle.client.ZuTransportClientProxy;
import zu.finagle.serialize.ThriftSerializer;
import zu.finagle.serialize.ZuSerializer;
import zu.finagle.server.ZuFinagleServer;
import zu.finagle.server.ZuTransportService;

import com.twitter.common.zookeeper.testing.BaseZooKeeperTest;
import com.twitter.finagle.Service;
import com.twitter.util.Future;

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
  
  
  @Test
  @SuppressWarnings("unchecked")
  public void testBasic() throws Exception {
    int port = 6100;
    
    ZuTransportService zuSvc = new ZuTransportService();
    zuSvc.registerHandler(new TestHandler());
    
    ZuFinagleServer server = new ZuFinagleServer(port, zuSvc.getService());
    
    server.start();
    Service<Req, Resp> svc = null;
    try {
      ZuClientFinagleServiceBuilder<Req, Resp> builder = new ZuClientFinagleServiceBuilder<Req, Resp>();
      ZuTransportClientProxy<Req, Resp> clientProxy = new ZuTransportClientProxy(TestHandler.SVC, TestHandler.serializer);
      svc = builder.clientProxy(clientProxy).host(new InetSocketAddress(port)).build();
      
      String s = "zu finagle test string";
      Req req = new Req();
      req.setS(s);
      Future<Resp> lenFuture = svc.apply(req);
      
      Resp resp = lenFuture.toJavaFuture().get();
      
      TestCase.assertEquals(s.length(), resp.getLen());
      
      req = new Req();
      lenFuture = svc.apply(req);
      
      resp = lenFuture.toJavaFuture().get();
      
      TestCase.assertEquals(0, resp.getLen());
    }
    finally {
      svc.close().toJavaFuture().get();
      server.shutdown();
    }
  }
}
