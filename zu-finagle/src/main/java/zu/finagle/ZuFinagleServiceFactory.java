package zu.finagle;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import zu.core.cluster.ZuServiceFactory;

import com.twitter.finagle.Service;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.thrift.ThriftClientFramedCodec;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.util.Duration;

public abstract class ZuFinagleServiceFactory implements ZuServiceFactory<ZuFinagleService> {

  private final int numThreads;
  private final long timeout;
  
  public ZuFinagleServiceFactory(int numThreads, long timeout){
    this.numThreads = numThreads;
    this.timeout = timeout;
  }
  
  @Override
  public ZuFinagleService getService(InetSocketAddress addr) {
    Service<ThriftClientRequest, byte[]> client =
        ClientBuilder.safeBuild(ClientBuilder.get()
                .hosts(addr)
            .codec(ThriftClientFramedCodec.get())
            .requestTimeout(Duration.apply(timeout, TimeUnit.MILLISECONDS))
            .hostConnectionLimit(numThreads));
    
    return getService(client);
  }
  
  abstract public ZuFinagleService getService(Service<ThriftClientRequest,byte[]> client);

}

