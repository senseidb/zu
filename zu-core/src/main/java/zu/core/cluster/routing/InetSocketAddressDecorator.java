package zu.core.cluster.routing;

import java.net.InetSocketAddress;
import java.util.Set;

public interface InetSocketAddressDecorator<T> {
  T decorate(InetSocketAddress addr);
  void cleanup(Set<T> toBeClosed);
}
