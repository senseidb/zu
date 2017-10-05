package zu.core.cluster;

import java.util.Set;

public interface ClusterManager {
  Cluster getCluster(String clusterName);
  Set<String> getAvailableClusters();
  String getClusterPrefix();
  void shutdown();
}