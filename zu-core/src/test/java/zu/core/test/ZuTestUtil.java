package zu.core.test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import zu.core.cluster.PartitionInfoReader;

public class ZuTestUtil{

  public static final Map<Integer,List<Integer>> CLUSTER_VIEW;
  
  public static PartitionInfoReader PartitionReader = new PartitionInfoReader() {
    
    @Override
    public List<Integer> getPartitionFor(InetSocketAddress addr) {
      int port = addr.getPort();
      return CLUSTER_VIEW.get(port);
    }
  };
  
  static{
    CLUSTER_VIEW = new HashMap<Integer,List<Integer>>();
    CLUSTER_VIEW.put(1, Arrays.asList(0,1));
    CLUSTER_VIEW.put(2, Arrays.asList(1,2));
    CLUSTER_VIEW.put(3, Arrays.asList(2,3));
  }
}
