package zu.finagle.client;

import com.twitter.finagle.Service;
import com.twitter.finagle.thrift.ThriftClientRequest;

public interface ZuClientProxy<Req, Resp>{
  Service<Req, Resp> wrap(Service<ThriftClientRequest, byte[]> client);
}
