package zu.finagle.client;

import org.apache.thrift.protocol.TBinaryProtocol;

import scala.runtime.BoxedUnit;
import zu.finagle.rpc.ZuThriftService;
import zu.finagle.rpc.ZuTransport;
import zu.finagle.serialize.ZuSerializer;

import com.twitter.finagle.Service;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.util.Future;
import com.twitter.util.Time;

public class ZuTransportClientProxy<Req, Resp> implements ZuClientProxy<Req, Resp>{
  
  private final ZuSerializer<Req, Resp> serializer;
  private final String name;
  
  public ZuTransportClientProxy(String name, ZuSerializer<Req, Resp> serializer) {
    this.serializer = serializer;
    this.name = name;
  }
  
  @Override
  public Service<Req, Resp> wrap(final Service<ThriftClientRequest, byte[]> client) {
    final ZuThriftService.ServiceIface svc = new ZuThriftService.ServiceToClient(client, new TBinaryProtocol.Factory());
    return new Service<Req, Resp>() {
      @Override
      public Future<BoxedUnit> close(Time deadline) {
        return client.close(deadline);
      }
      
      @Override
      public Future<Resp> apply(Req req) {
        try {
          ZuTransport reqTransport = new ZuTransport();
          reqTransport.setName(name);
          reqTransport.setData(serializer.serializeRequest(req));
          Future<ZuTransport> future = svc.send(reqTransport);
        
          ZuTransport resTransport = future.apply();
        
          Resp resp = serializer.deserializeResponse(resTransport.data);
        
          return Future.value(resp);
        }
        catch(Exception e) {
          return Future.exception(e);
        }
      }
    };
  }
}
