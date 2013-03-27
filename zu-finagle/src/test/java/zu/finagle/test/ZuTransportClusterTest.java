package zu.finagle.test;

import java.util.Arrays;
import java.util.HashSet;

import junit.framework.TestCase;

import org.junit.Test;

import zu.finagle.client.ZuClientFinagleServiceBuilder;

import com.twitter.finagle.Service;
import com.twitter.util.Future;

public class ZuTransportClusterTest extends ZuClusterTestBase {
  

  @Override
  protected ClusterType getClusterType() {
    return ClusterType.ZuTransport;
  }
  
  @Test
  public void testCluster2() throws Exception{
    ZuClientFinagleServiceBuilder<Req2, Resp2> builder = new ZuClientFinagleServiceBuilder<Req2, Resp2>();
    Service<Req2, Resp2> svc = builder.scatterGather(scatterGather).routingAlgorithm(routingAlgorithm).build();
      
    Future<Resp2> future = svc.apply(new Req2());
    Resp2 merged = future.apply();
    TestCase.assertEquals(new HashSet<Integer>(Arrays.asList(0,1,2,3)), merged.getVals());
  }
}
