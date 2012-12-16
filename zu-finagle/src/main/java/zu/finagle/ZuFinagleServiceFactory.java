package zu.finagle;

import java.net.InetSocketAddress;


import com.twitter.finagle.Service;

public abstract class ZuFinagleServiceFactory<Req,Res>{

  protected final int numThreads;
  protected final long timeout;
  
  public ZuFinagleServiceFactory(int numThreads, long timeout){
    this.numThreads = numThreads;
    this.timeout = timeout;
  }
  
  public abstract Service<Req,Res> buildFinagleService(InetSocketAddress addr);
}
