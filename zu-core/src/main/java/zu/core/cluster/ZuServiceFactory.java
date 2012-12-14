package zu.core.cluster;

import java.net.InetSocketAddress;

public interface ZuServiceFactory<S extends ZuService> {
  S getService(InetSocketAddress addr);
}
