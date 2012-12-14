package zu.finagle;

import java.net.InetSocketAddress;
import java.util.List;

import zu.core.cluster.ZuService;

import com.twitter.finagle.Service;
import com.twitter.finagle.thrift.ThriftClientRequest;

public abstract class ZuFinagleService implements ZuService {
  private final Service<ThriftClientRequest, byte[]> finagleSvc;
  private final InetSocketAddress addr;
  
  public ZuFinagleService(Service<ThriftClientRequest, byte[]> finagleSvc, InetSocketAddress addr){
    this.finagleSvc = finagleSvc;
    this.addr = addr;
  }
  
  @Override
  public abstract List<Integer> getPartitions();

  @Override
  public InetSocketAddress getAddress() {
    return addr;
  }

  @Override
  public void shutdown() {
    finagleSvc.release();
  }

}
