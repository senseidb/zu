package zu.core.cluster;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * a cluster listener
 */
public interface ZuClusterEventListener {
  /**
   * called when the cluster topology changes.
   * @param clusterView the current view of the cluster.
   */
  default public void clusterChanged(Map<Integer, List<InetSocketAddress>> clusterView){
    
  }
  
  /**
   * called for a set of nodes removed from the cluster.
   * @param removedNodes list of nodes removed from the cluster
   */
  default public void nodesRemoved(Set<InetSocketAddress> removedNodes) {
    
  }
}
