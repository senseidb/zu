package zu.core.cluster;

import java.net.InetSocketAddress;
import java.util.List;

public interface ZuService {
  List<Integer> getPartitions();
  InetSocketAddress getAddress();
  void shutdown();
}
