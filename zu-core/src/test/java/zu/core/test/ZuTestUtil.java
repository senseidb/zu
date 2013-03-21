package zu.core.test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZuTestUtil{

  public static final Map<Integer,List<Integer>> CLUSTER_VIEW;
    
  static{
    CLUSTER_VIEW = new HashMap<Integer,List<Integer>>();
    CLUSTER_VIEW.put(1, Arrays.asList(0,1));
    CLUSTER_VIEW.put(2, Arrays.asList(1,2));
    CLUSTER_VIEW.put(3, Arrays.asList(2,3));
  }
}
