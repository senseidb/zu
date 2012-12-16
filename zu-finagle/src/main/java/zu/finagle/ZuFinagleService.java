package zu.finagle;

import java.net.InetSocketAddress;

import zu.core.cluster.ZuService;

import com.twitter.finagle.Service;

public class ZuFinagleService<Req,Res> implements ZuService {
  private final Service<Req, Res> finagleSvc;
  private final InetSocketAddress addr;
  
  public ZuFinagleService(Service<Req,Res> finagleSvc, InetSocketAddress addr){
    this.finagleSvc = finagleSvc;
    this.addr = addr;
  }
  
  @Override
  public InetSocketAddress getAddress() {
    return addr;
  }
  
  public Service<Req,Res> getFinagleSvc(){
    return finagleSvc;
  }

  @Override
  public void shutdown() {
    finagleSvc.release();
  }

}
