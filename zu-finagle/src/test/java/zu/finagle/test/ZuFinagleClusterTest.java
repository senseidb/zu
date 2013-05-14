package zu.finagle.test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;

import junit.framework.TestCase;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Before;
import org.junit.Test;

import zu.finagle.client.ZuClientFinagleServiceBuilder;
import zu.finagle.client.ZuFinagleServiceDecorator;
import zu.finagle.client.ZuTransportClientProxy;
import zu.finagle.server.ZuFinagleServer;

import com.twitter.finagle.Service;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.thrift.ThriftClientFramedCodec;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.util.Future;

public class ZuFinagleClusterTest extends ZuClusterTestBase {
  
  private ZuClientFinagleServiceBuilder<Req2, Resp2> builder;
  private Service<Req2, Resp2> svc;
  
  @Before
  public void setup() throws Exception {
    super.setup();
    builder = new ZuClientFinagleServiceBuilder<Req2, Resp2>();
    svc = builder.scatterGather(scatterGather).routingAlgorithm(routingAlgorithm).build();
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void testBrokerAsZuTransportService() throws Exception {
    int brokerPort = 6666;
    ZuFinagleServer broker = null;
    
    try {
      broker = ZuFinagleServer.buildBroker(ReqServiceImpl.SVC, brokerPort, svc, ReqServiceImpl.serializer);
      
      broker.start();
      
      ZuTransportClientProxy<Req2, Resp2> brokerClientProxy = new ZuTransportClientProxy(ReqServiceImpl.SVC, ReqServiceImpl.serializer);
      
      Service<Req2, Resp2> brokerClient = new ZuFinagleServiceDecorator<Req2, Resp2>(brokerClientProxy).decorate(new InetSocketAddress(brokerPort));
      
      Future<Resp2> future  = brokerClient.apply(new Req2());
      Resp2 merged = future.apply();
      TestCase.assertEquals(new HashSet<Integer>(Arrays.asList(0,1,2,3)), merged.getVals());
      
      brokerClient.close(); 
    }
    finally {
      if (broker != null) {
        broker.shutdown();
      }
      
    }
  }
  
  @Test
  public void testBrokerAsFinagleService() throws Exception {
    int brokerPort = 6667;
    ZuFinagleServer broker = null;
    
    try {
      ReqService.ServiceIface svcImpl = new ReqService.ServiceIface() {

        @Override
        public Future<Resp2> handle(Req2 req) {
          return svc.apply(req);
        }
        
      };
      
      Service<byte[],byte[]> brokerSvc = new ReqService.Service(svcImpl, new TBinaryProtocol.Factory());
      
      
      broker = new ZuFinagleServer(brokerPort, brokerSvc);
      
      broker.start();
      
      Service<ThriftClientRequest, byte[]> client = ClientBuilder.safeBuild(ClientBuilder.get()
          .hosts(new InetSocketAddress(brokerPort))
          .codec(ThriftClientFramedCodec.get())
          .hostConnectionLimit(ZuClientFinagleServiceBuilder.DEFAULT_NUM_THREADS));
      
      ReqService.ServiceIface brokerClient = new ReqService.ServiceToClient(client, new TBinaryProtocol.Factory());
      
      Future<Resp2> future  = brokerClient.handle(new Req2());
      Resp2 merged = future.apply();
      TestCase.assertEquals(new HashSet<Integer>(Arrays.asList(0,1,2,3)), merged.getVals());
      
      client.close();
    }
    finally {
      if (broker != null) {
        broker.shutdown();
      }
      
    }
  }
  
  @Test
  public void testScatterGather() throws Exception {
    Future<Resp2> future = svc.apply(new Req2());
    Resp2 merged = future.apply();
    TestCase.assertEquals(new HashSet<Integer>(Arrays.asList(0,1,2,3)), merged.getVals());
  }

  @Override
  protected ClusterType getClusterType() {
    return ClusterType.Finagle;
  }
}
