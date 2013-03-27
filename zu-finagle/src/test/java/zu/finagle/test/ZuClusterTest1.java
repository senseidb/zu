package zu.finagle.test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Before;
import org.junit.Test;

import scala.runtime.BoxedUnit;
import zu.core.cluster.routing.RoutingAlgorithm;
import zu.finagle.client.ZuClientFinagleServiceBuilder;
import zu.finagle.client.ZuClientProxy;
import zu.finagle.client.ZuFinagleServiceDecorator;
import zu.finagle.client.ZuScatterGatherer;
import zu.finagle.client.ZuTransportClientProxy;
import zu.finagle.server.ZuFinagleServer;

import com.twitter.finagle.Service;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.util.Future;
import com.twitter.util.Time;

public class ZuClusterTest1 extends ZuFinagleClusterTest {

  private ZuScatterGatherer<Req2, Resp2> scatterGather;
  private RoutingAlgorithm<Service<Req2, Resp2>> routingAlgorithm;
  private ZuClientProxy<Req2, Resp2> clientProxy;
  
  protected ZuFinagleServer buildServer(TestClusterHandler handler, int port) {
    return new ZuFinagleServer(port, handler.getService());
  }
  
  @Before
  public void init() throws Exception{
    super.init();
    clientProxy = new ZuClientProxy<Req2, Resp2>() {
      
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
    
    ZuFinagleServiceDecorator<Req2, Resp2> socketDecorator = new ZuFinagleServiceDecorator<Req2, Resp2>(clientProxy);
    routingAlgorithm = new RoutingAlgorithm.RandomAlgorithm<>(socketDecorator);
        
    cluster.addClusterEventListener(routingAlgorithm);
    
    startServers();
    
    
    scatterGather = new ZuScatterGatherer<Req2, Resp2>(){
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
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void testBroker() throws Exception {
    int brokerPort = 6666;
    ZuFinagleServer broker = null;
    
    try {
      ZuClientFinagleServiceBuilder<Req2, Resp2> builder = new ZuClientFinagleServiceBuilder<Req2, Resp2>();
      Service<Req2, Resp2> svc = builder.scatterGather(scatterGather).routingAlgorithm(routingAlgorithm).build();
      
      broker = ZuFinagleServer.buildBroker(TestClusterHandler.SVC, brokerPort, svc, TestClusterHandler.serializer);
      
      broker.start();
      
      ZuTransportClientProxy<Req2, Resp2> brokerClientProxy = new ZuTransportClientProxy<>(TestClusterHandler.SVC, TestClusterHandler.serializer);
      
      
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
  public void testBroker2() throws Exception {
    int brokerPort = 6667;
    ZuFinagleServer broker = null;
    
    try {
      ZuClientFinagleServiceBuilder<Req2, Resp2> builder = new ZuClientFinagleServiceBuilder<Req2, Resp2>();
      final Service<Req2, Resp2> svc = builder.scatterGather(scatterGather).routingAlgorithm(routingAlgorithm).build();
      
      TestService.ServiceIface svcImpl = new TestService.ServiceIface() {

        @Override
        public Future<Resp2> handle(Req2 req) {
          return svc.apply(req);
        }
        
      };
      
      Service<byte[],byte[]> brokerSvc = new TestService.Service(svcImpl, new TBinaryProtocol.Factory());
      
      
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
  public void testCluster1() throws Exception {
    ZuClientFinagleServiceBuilder<Req2, Resp2> builder = new ZuClientFinagleServiceBuilder<Req2, Resp2>();
    
    Service<Req2, Resp2> svc = builder.scatterGather(scatterGather).routingAlgorithm(routingAlgorithm).build();
    
    Future<Resp2> future = svc.apply(new Req2());
    Resp2 merged = future.apply();
    TestCase.assertEquals(new HashSet<Integer>(Arrays.asList(0,1,2,3)), merged.getVals());
  }
}
