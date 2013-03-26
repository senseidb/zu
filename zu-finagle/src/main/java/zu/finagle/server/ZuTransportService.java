package zu.finagle.server;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.protocol.TBinaryProtocol;

import zu.finagle.rpc.ZuThriftService;
import zu.finagle.rpc.ZuTransport;
import zu.finagle.serialize.ZuSerializer;

import com.twitter.finagle.Service;
import com.twitter.util.Future;

public class ZuTransportService implements ZuThriftService.ServiceIface{
  private final Map<String,RequestHandler<?,?>> reqHandlerMap;
  
  public interface RequestHandler<Req, Res> {
    String getName();
    Res handleRequest(Req req);
    ZuSerializer<Req, Res> getSerializer();
  }
  
  public ZuTransportService() {
    reqHandlerMap = new HashMap<String,RequestHandler<?,?>>();
  }
  
  public <Req, Res> ZuTransportService registerHandler(RequestHandler<Req, Res> reqHandler) {
    reqHandlerMap.put(reqHandler.getName(), reqHandler);
    return this;
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Future<ZuTransport> send(ZuTransport req) {
    String name = req.getName();
    RequestHandler handler = reqHandlerMap.get(name);
    if (handler == null) {
      return Future.exception(new IllegalArgumentException("handler "+name+" is not registered"));
    }
    try {
      ZuSerializer serializer = handler.getSerializer();
      Object reqObj = serializer.deserializeRequest(req.data);
      Object res = handler.handleRequest(reqObj);
      ByteBuffer bytes = serializer.serializeResponse(res);
    
      ZuTransport resp = new ZuTransport();
      resp.setName(name);
      resp.setData(bytes);
      return Future.value(resp);
    }
    catch(Throwable th) {
      return Future.exception(th);
    }
  }
  
  public Service<byte[], byte[]> getService() {
    return new ZuThriftService.Service(this, new TBinaryProtocol.Factory());
  }
}
