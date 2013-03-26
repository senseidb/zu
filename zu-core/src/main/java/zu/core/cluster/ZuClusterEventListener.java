package zu.core.cluster;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

/**
 * a cluster listener
 */
public interface ZuClusterEventListener {
  /**
   * called the the cluster topology changes.
   * @param clusterView the current view of the cluster.
   */
  public void clusterChanged(Map<Integer, List<InetSocketAddress>> clusterView);
  
}
