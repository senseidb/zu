package zu.finagle.client;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import zu.core.cluster.routing.RoutingAlgorithm;
import zu.finagle.serialize.ZuSerializer;

import com.twitter.finagle.Service;
import com.twitter.util.Duration;
import com.twitter.util.Future;


public final class ZuClientFinagleServiceFactory{
  
  private Map<String, ZuSerializer<?,?>> serializerMap = new HashMap<String, ZuSerializer<?,?>>();
  
  
  public ZuClientFinagleServiceFactory() {
  }
  
  public <Req, Res> void registerSerializer(String name, ZuSerializer<Req, Res> serializer){
    serializerMap.put(name, serializer);
  }
  
  public <Req, Res> Service<Req,Res> getService(final String name, final ZuScatterGatherer<Req,Res> scatterGather, 
      final RoutingAlgorithm<Service<Req, Res>> routingAlgorithm, final Set<Integer> shards, final byte[] routingKey){
    
    return new Service<Req, Res>(){

      @Override
      public Future<Res> apply(Req req) {
        
        return null;
      }
      
    };
    
  }
  
  @SuppressWarnings("unchecked")
  public <Req, Res> Service<Req,Res> getService(final String name, InetSocketAddress addr, Duration timeout, int numThreads){
    final ZuSerializer<Req, Res> serializer = (ZuSerializer<Req, Res>)serializerMap.get(name);
    if (serializer == null) {
      throw new IllegalArgumentException("unrecognized serializer: "+name);
    }
    
    ZuFinagleServiceDecorator<Req, Res> builder = new ZuFinagleServiceDecorator<Req, Res>(name, serializer, timeout, numThreads);
    return builder.decorate(addr);
  }
}
