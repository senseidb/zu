package zu.core.cluster;

import java.net.InetSocketAddress;

public interface ZuService {
  InetSocketAddress getAddress();
  void shutdown();
}
