package zu.finagle.client;

import java.net.InetSocketAddress;

import org.apache.thrift.protocol.TBinaryProtocol;

import zu.core.cluster.routing.InetSocketAddressDecorator;
import zu.finagle.rpc.ZuThriftService;
import zu.finagle.rpc.ZuTransport;
import zu.finagle.serialize.ZuSerializer;

import com.twitter.finagle.Service;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.thrift.ThriftClientFramedCodec;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.util.Duration;
import com.twitter.util.Future;

public class ZuFinagleServiceDecorator<Req, Res> implements InetSocketAddressDecorator<Service<Req,Res>>{

  private final ZuSerializer<Req, Res> serializer;
  private final Duration timeout;
  private final int numThreads;
  private final String name;
  
  public ZuFinagleServiceDecorator(String name, ZuSerializer<Req, Res> serializer) {
    this(name, serializer, ZuClientFinagleServiceBuilder.DEFAULT_TIMEOUT_DURATION, ZuClientFinagleServiceBuilder.DEFAULT_NUM_THREADS);
  }
  
  public ZuFinagleServiceDecorator(String name, ZuSerializer<Req, Res> serializer, Duration timeout, int numThreads){
    this.serializer = serializer;
    this.timeout = timeout;
    this.numThreads = numThreads;
    this.name = name;
  }
  
  @Override
  public Service<Req, Res> decorate(InetSocketAddress addr) {
    Service<ThriftClientRequest, byte[]> client = ClientBuilder.safeBuild(ClientBuilder.get().hosts(addr)
    .codec(ThriftClientFramedCodec.get())
    .requestTimeout(timeout)
    .hostConnectionLimit(numThreads));
    
    final ZuThriftService.ServiceIface svc = new ZuThriftService.ServiceToClient(client, new TBinaryProtocol.Factory());
    
    return new Service<Req, Res>(){
      

      @Override
      public Future<Res> apply(Req req) {
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
