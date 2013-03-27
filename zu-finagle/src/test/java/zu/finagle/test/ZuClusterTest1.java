package zu.finagle.test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;

import junit.framework.TestCase;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Before;
import org.junit.Test;

import scala.runtime.BoxedUnit;
import zu.core.cluster.routing.RoutingAlgorithm;
import zu.finagle.client.ZuClientFinagleServiceBuilder;
import zu.finagle.client.ZuClientProxy;
import zu.finagle.client.ZuFinagleServiceDecorator;
import zu.finagle.client.ZuTransportClientProxy;
import zu.finagle.server.ZuFinagleServer;

import com.twitter.finagle.Service;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.util.Future;
import com.twitter.util.Time;

public class ZuClusterTest1 extends ZuFinagleClusterTest {
  
  @Test
  @SuppressWarnings("unchecked")
  public void testBrokerAsZuTransportService() throws Exception {
    int brokerPort = 6666;
    ZuFinagleServer broker = null;
    
    try {
      ZuClientFinagleServiceBuilder<Req2, Resp2> builder = new ZuClientFinagleServiceBuilder<Req2, Resp2>();
      Service<Req2, Resp2> svc = builder.scatterGather(scatterGather).routingAlgorithm(routingAlgorithm).build();
      
      broker = ZuFinagleServer.buildBroker(ReqServiceImpl.SVC, brokerPort, svc, ReqServiceImpl.serializer);
      
      broker.start();
      
      ZuTransportClientProxy<Req2, Resp2> brokerClientProxy = new ZuTransportClientProxy<>(ReqServiceImpl.SVC, ReqServiceImpl.serializer);
      
      
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
      ZuClientFinagleServiceBuilder<Req2, Resp2> builder = new ZuClientFinagleServiceBuilder<Req2, Resp2>();
      final Service<Req2, Resp2> svc = builder.scatterGather(scatterGather).routingAlgorithm(routingAlgorithm).build();
      
      ReqService.ServiceIface svcImpl = new ReqService.ServiceIface() {

        @Override
        public Future<Resp2> handle(Req2 req) {
          return svc.apply(req);
        }
        
      };
      
      Service<byte[],byte[]> brokerSvc = new ReqService.Service(svcImpl, new TBinaryProtocol.Factory());
      
      
      broker = new ZuFinagleServer(brokerPort, brokerSvc);
      
      broker.start();
      
      ZuClientFinagleServiceBuilder<Req2, Resp2> clientBuilder = new ZuClientFinagleServiceBuilder<Req2, Resp2>();
      Service<Req2, Resp2> brokerClient = clientBuilder.host(new InetSocketAddress(brokerPort)).clientProxy(clientProxy).build();
      
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
  public void testScatterGather() throws Exception {
    ZuClientFinagleServiceBuilder<Req2, Resp2> builder = new ZuClientFinagleServiceBuilder<Req2, Resp2>();
    
    Service<Req2, Resp2> svc = builder.scatterGather(scatterGather).routingAlgorithm(routingAlgorithm).build();
    
    Future<Resp2> future = svc.apply(new Req2());
    Resp2 merged = future.apply();
    TestCase.assertEquals(new HashSet<Integer>(Arrays.asList(0,1,2,3)), merged.getVals());
  }

  @Override
  protected ClusterType getClusterType() {
    return ClusterType.Finagle;
  }
}
