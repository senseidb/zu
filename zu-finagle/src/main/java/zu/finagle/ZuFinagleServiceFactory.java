package zu.finagle;

import java.net.InetSocketAddress;

import zu.core.cluster.ZuServiceFactory;

import com.twitter.finagle.Service;

public abstract class ZuFinagleServiceFactory<Req,Res> implements ZuServiceFactory<ZuFinagleService<Req,Res>> {

  protected final int numThreads;
  protected final long timeout;
  
  public ZuFinagleServiceFactory(int numThreads, long timeout){
    this.numThreads = numThreads;
    this.timeout = timeout;
  }
  
  protected abstract Service<Req,Res> buildFinagleService(InetSocketAddress addr);
  
  @Override
  public ZuFinagleService<Req,Res> getService(InetSocketAddress addr) {
    return new ZuFinagleService<Req,Res>(buildFinagleService(addr), addr);
  }
}

