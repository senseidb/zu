package zu.core.test;

import java.net.InetSocketAddress;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import zu.core.cluster.ZookeeperClientBuilder;

public class ZookeeperClientBuilderTest {
  static void checkInetAddress(String host, int port, InetSocketAddress addr) {
    Assert.assertNotNull(addr);
    Assert.assertEquals(host, addr.getHostName());
    Assert.assertEquals(host, addr.getHostString());
    Assert.assertEquals(port, addr.getPort());
  }
  
  @Test
  public void testParseBadHostPort() {
    InetSocketAddress addr = ZookeeperClientBuilder.parseHostPort(null);
    Assert.assertNull(addr);
    
    addr = ZookeeperClientBuilder.parseHostPort("");
    Assert.assertNull(addr);
    
    addr = ZookeeperClientBuilder.parseHostPort("localhost");
    Assert.assertNull(addr);
    
    addr = ZookeeperClientBuilder.parseHostPort("localhost:");
    Assert.assertNull(addr);
    
    addr = ZookeeperClientBuilder.parseHostPort("localhost:abc");
    Assert.assertNull(addr);
  }
  
  @Test
  public void testParseHostPort() {
    InetSocketAddress addr = ZookeeperClientBuilder.parseHostPort("localhost:1234");
    checkInetAddress("localhost", 1234, addr);
  }
  
  @Test
  public void testParseZookeeperUrl() {
    List<InetSocketAddress> addrList = ZookeeperClientBuilder.parseZookeeperUrl("localhost:1234");
    Assert.assertEquals(1, addrList.size());
    checkInetAddress("localhost", 1234, addrList.get(0));
    
    addrList = ZookeeperClientBuilder.parseZookeeperUrl("localhost:1234,");
    Assert.assertEquals(1, addrList.size());
    checkInetAddress("localhost", 1234, addrList.get(0));
    
    addrList = ZookeeperClientBuilder.parseZookeeperUrl("");
    Assert.assertEquals(0, addrList.size());
    
    addrList = ZookeeperClientBuilder.parseZookeeperUrl(null);
    Assert.assertEquals(0, addrList.size());
    
    addrList = ZookeeperClientBuilder.parseZookeeperUrl("localhost:1234,farhost:5678");
    Assert.assertEquals(2, addrList.size());
    checkInetAddress("localhost", 1234, addrList.get(0));
    checkInetAddress("farhost", 5678, addrList.get(1));
  }
}
