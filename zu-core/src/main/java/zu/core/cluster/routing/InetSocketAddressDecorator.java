package zu.core.cluster.routing;

import java.net.InetSocketAddress;

public interface InetSocketAddressDecorator<T> {
  T decorate(InetSocketAddress addr);
}
