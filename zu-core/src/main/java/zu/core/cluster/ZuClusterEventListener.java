package zu.core.cluster;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public interface ZuClusterEventListener {
  public void clusterChanged(Map<Integer,ArrayList<InetSocketAddress>> clusterView);
  public void nodesRemovedFromCluster(List<InetSocketAddress> nodes);
}
