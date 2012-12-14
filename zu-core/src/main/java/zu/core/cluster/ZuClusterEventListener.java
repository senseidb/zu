package zu.core.cluster;

import java.util.ArrayList;
import java.util.Map;

public interface ZuClusterEventListener<S extends ZuService> {
  public void clusterChanged(Map<Integer,ArrayList<S>> clusterView);
}
