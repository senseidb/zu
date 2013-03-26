package zu.finagle;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.protocol.TBinaryProtocol;

import zu.finagle.rpc.ZuThriftService;
import zu.finagle.rpc.ZuTransport;
import zu.finagle.serialize.ZuSerializer;

import com.twitter.finagle.Service;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.thrift.ThriftClientFramedCodec;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.util.Duration;
import com.twitter.util.Future;


public final class ZuClientFinagleServiceFactory{
  
  private Map<String, ZuSerializer<?,?>> serializerMap = new HashMap<String, ZuSerializer<?,?>>();
  
  private final ZuThriftService.ServiceIface svc;
  
  public ZuClientFinagleServiceFactory(InetSocketAddress addr, long timeoutInMillis, int numThreads) {
    Service<ThriftClientRequest, byte[]> client = ClientBuilder.safeBuild(ClientBuilder.get()
        .hosts(addr)
    .codec(ThriftClientFramedCodec.get())
    .requestTimeout(Duration.apply(timeoutInMillis, TimeUnit.MILLISECONDS))
    .hostConnectionLimit(numThreads));
    svc = new ZuThriftService.ServiceToClient(client, new TBinaryProtocol.Factory());
  }
  
  public <Req, Res> void registerSerializer(String name, ZuSerializer<Req, Res> serializer){
    serializerMap.put(name, serializer);
  }
  
  @SuppressWarnings("unchecked")
  public <Req, Res> Service<Req,Res> getService(final String name){
    final ZuSerializer<Req, Res> serializer = (ZuSerializer<Req, Res>)serializerMap.get(name);
    return new Service<Req, Res>(){

      @Override
      public Future<Res> apply(Req req) {
        if (serializer == null) {
          return Future.exception(new IllegalArgumentException("unrecognized serializer: "+name));
        }
        
        try {
          ZuTransport reqTransport = new ZuTransport();
          reqTransport.setName(name);
          reqTransport.setData(serializer.serializeRequest(req));
          Future<ZuTransport> future = svc.send(reqTransport);
        
          ZuTransport resTransport = future.apply();
        
          Res resp = serializer.deserializeResponse(resTransport.data);
        
          return Future.value(resp);
        }
        catch(Exception e) {
          return Future.exception(e);
        }
      }
      
    };
  }
}
