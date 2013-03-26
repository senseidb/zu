package zu.finagle.server;

import com.twitter.finagle.Service;

public interface ServiceBuilder{
  public Service<byte[], byte[]> build();
}
