package zu.finagle.client;

import java.net.InetSocketAddress;


import com.twitter.finagle.Service;

public abstract class ZuFinagleServiceFactory{

  protected final int numThreads;
  protected final long timeout;
  
  public ZuFinagleServiceFactory(int numThreads, long timeout){
    this.numThreads = numThreads;
    this.timeout = timeout;
  }
  
  public abstract <Req,Res> Service<Req,Res> buildFinagleService(InetSocketAddress addr);
}
