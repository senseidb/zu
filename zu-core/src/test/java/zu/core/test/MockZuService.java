package zu.core.test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import zu.core.cluster.ZuService;
import zu.core.cluster.ZuServiceFactory;

public class MockZuService implements ZuService{

  public static Map<Integer,List<Integer>> CLUSTER_VIEW;
  
  public static ZuServiceFactory<MockZuService> Factory = new ZuServiceFactory<MockZuService>() {

    @Override
    public MockZuService getService(InetSocketAddress addr) {
      return new MockZuService(addr);
    }
  };
  
  static{
    CLUSTER_VIEW = new HashMap<Integer,List<Integer>>();
    CLUSTER_VIEW.put(1, Arrays.asList(0,1));
    CLUSTER_VIEW.put(2, Arrays.asList(1,2));
    CLUSTER_VIEW.put(3, Arrays.asList(2,3));
  }
  
  private final InetSocketAddress addr;
  public MockZuService(InetSocketAddress addr){
    this.addr = addr;
  }

  @Override
  public List<Integer> getPartitions() {
    int port = addr.getPort();
    return CLUSTER_VIEW.get(port);
  }

  @Override
  public InetSocketAddress getAddress() {
    return addr;
  }

  @Override
  public void shutdown() {
    
  }
}
