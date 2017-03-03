package zu.core.cluster;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Cluster {
  String id();
  String namespace();
  Map<Integer,List<InetSocketAddress>> getClusterView();
  void addClusterEventListener(ZuClusterEventListener lsnr);
  ClusterRef join(InetSocketAddress addr, Set<Integer> shards) throws Exception;
  void shutdown();
}
