package zu.core.cluster;

import java.util.Map;
import java.util.Set;

import org.jboss.netty.util.internal.ConcurrentHashMap;

public class MockClusterManager implements ClusterManager {

  private final Map<String, Cluster> clusterMap = new ConcurrentHashMap<>();
  
  public void addCluster(String name, Cluster cluster) {
    clusterMap.put(name, cluster);
  }
  
  @Override
  public Cluster getCluster(String clusterName) {
    return clusterMap.get(clusterName);
  }

  @Override
  public Set<String> getAvailableClusters() {
    return clusterMap.keySet();
  }

  @Override
  public String getClusterPrefix() {
    return "";
  }

  @Override
  public void shutdown() {    
    clusterMap.clear();
  }
}
