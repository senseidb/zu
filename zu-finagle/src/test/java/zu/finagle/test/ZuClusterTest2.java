package zu.finagle.test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import junit.framework.TestCase;

import org.junit.Test;

import zu.core.cluster.routing.RoutingAlgorithm;
import zu.finagle.client.ZuClientFinagleServiceBuilder;
import zu.finagle.client.ZuFinagleServiceDecorator;
import zu.finagle.client.ZuScatterGatherer;
import zu.finagle.client.ZuTransportClientProxy;
import zu.finagle.server.ZuFinagleServer;
import zu.finagle.server.ZuTransportService;

import com.twitter.finagle.Service;
import com.twitter.util.Future;

public class ZuClusterTest2 extends ZuFinagleClusterTest {
  
  protected ZuFinagleServer buildServer(TestClusterHandler handler, int port) {
    ZuTransportService zuSvc = new ZuTransportService();
    zuSvc.registerHandler(handler);
    return new ZuFinagleServer(port, zuSvc.getService());
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void testCluster2() throws Exception{
    
    ZuTransportClientProxy<Integer, HashSet<Integer>> clientProxy = new ZuTransportClientProxy<>(TestClusterHandler.SVC, TestClusterHandler.serializer);
    
    RoutingAlgorithm<Service<Integer, HashSet<Integer>>> routingAlgorithm = 
        new RoutingAlgorithm.RandomAlgorithm<>(new ZuFinagleServiceDecorator<Integer, HashSet<Integer>>(clientProxy));
        
    cluster.addClusterEventListener(routingAlgorithm);
    
    startServers();
    
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
    
    
    ZuClientFinagleServiceBuilder<Integer, HashSet<Integer>> builder = new ZuClientFinagleServiceBuilder<Integer, HashSet<Integer>>();
    Service<Integer, HashSet<Integer>> svc = builder.scatterGather(scatterGather).routingAlgorithm(routingAlgorithm).build();
      
    Future<HashSet<Integer>> future = svc.apply(0);
    HashSet<Integer> merged = future.apply();
    TestCase.assertEquals(new HashSet<Integer>(Arrays.asList(0,1,2,3)), merged);
  }
}
