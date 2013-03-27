package zu.finagle.test;

import java.util.Arrays;
import java.util.HashSet;

import junit.framework.TestCase;

import org.junit.Test;

import zu.core.cluster.routing.RoutingAlgorithm;
import zu.finagle.client.ZuClientFinagleServiceBuilder;
import zu.finagle.client.ZuFinagleServiceDecorator;
import zu.finagle.client.ZuTransportClientProxy;
import zu.finagle.server.ZuFinagleServer;
import zu.finagle.server.ZuTransportService;
import zu.finagle.test.ZuFinagleClusterTest.ClusterType;

import com.twitter.finagle.Service;
import com.twitter.util.Future;

public class ZuClusterTest2 extends ZuFinagleClusterTest {
  

  @Override
  protected ClusterType getClusterType() {
    return ClusterType.ZuTransport;
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void testCluster2() throws Exception{
    ZuClientFinagleServiceBuilder<Req2, Resp2> builder = new ZuClientFinagleServiceBuilder<Req2, Resp2>();
    Service<Req2, Resp2> svc = builder.scatterGather(scatterGather).routingAlgorithm(routingAlgorithm).build();
      
    Future<Resp2> future = svc.apply(new Req2());
    Resp2 merged = future.apply();
    TestCase.assertEquals(new HashSet<Integer>(Arrays.asList(0,1,2,3)), merged.getVals());
  }
}
