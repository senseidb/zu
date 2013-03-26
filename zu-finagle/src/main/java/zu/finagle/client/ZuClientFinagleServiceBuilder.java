package zu.finagle.client;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import zu.core.cluster.routing.RoutingAlgorithm;
import zu.finagle.serialize.ZuSerializer;

import com.twitter.finagle.Service;
import com.twitter.util.Duration;
import com.twitter.util.Future;


public final class ZuClientFinagleServiceBuilder<Req, Res>{
  
  static final int DEFAULT_NUM_THREADS = 8;  // default thread count
  static final Duration DEFAULT_PARTIAL_TIMEOUT_DURATION = Duration.apply(10, TimeUnit.SECONDS);
  static final Duration DEFAULT_TIMEOUT_DURATION = Duration.apply(10, TimeUnit.SECONDS);
  
  private Duration timeout = DEFAULT_TIMEOUT_DURATION;
  private InetSocketAddress host = null;
  private String name = null;
  private ZuSerializer<Req, Res> serializer = null;
  private ZuScatterGatherer<Req,Res> scatterGather = null;
  private Set<Integer> shards = null;
  private byte[] routingKey = null;
  private Duration partialResultTimeout = DEFAULT_PARTIAL_TIMEOUT_DURATION;
  private int numThreads = DEFAULT_NUM_THREADS;
  private RoutingAlgorithm<Service<Req, Res>> routingAlgorithm = null;
  
  public ZuClientFinagleServiceBuilder<Req, Res> name(String name) {
    this.name = name;
    return this;
  }
  
  public ZuClientFinagleServiceBuilder<Req, Res> host(InetSocketAddress host) {
    this.host = host;
    return this;
  }
  
  public ZuClientFinagleServiceBuilder<Req, Res> routingAlgorithm(RoutingAlgorithm<Service<Req, Res>> routingAlgorithm) {
   this.routingAlgorithm = routingAlgorithm;
   return this;
  }
  
  public ZuClientFinagleServiceBuilder<Req, Res> serializer(ZuSerializer<Req, Res> serializer) {
    this.serializer = serializer;
    return this;
  }
  
  public ZuClientFinagleServiceBuilder<Req, Res> numThreads(int numThreads) {
    this.numThreads = numThreads;
    return this;
  }
  
  public ZuClientFinagleServiceBuilder<Req, Res> partialResultTimeout(Duration partialResultTimeout) {
    this.partialResultTimeout = partialResultTimeout;
    return this;
  }
  
  public ZuClientFinagleServiceBuilder<Req, Res> timeout(Duration timeout) {
    this.timeout = timeout;
    return this;
  }
  
  public ZuClientFinagleServiceBuilder<Req, Res> routingKey(byte[] routingKey) {
    this.routingKey = routingKey;
    return this;
  }
  
  public ZuClientFinagleServiceBuilder<Req, Res> shards(Set<Integer> shards) {
    this.shards = shards;
    return this;
  }
  
  public ZuClientFinagleServiceBuilder<Req, Res> scatterGather(ZuScatterGatherer<Req,Res> scatterGather) {
    this.scatterGather = scatterGather;
    return this;
  }
  
  public Service<Req, Res> build(){
    if (name == null || serializer == null) {
      throw new IllegalArgumentException("both name and serializer must be supplied");
    }
    
    if (host != null) {
      ZuFinagleServiceDecorator<Req, Res> decorator = new ZuFinagleServiceDecorator<Req, Res>(name, serializer, timeout, numThreads);
      return decorator.decorate(host);
    }
    
    if (routingAlgorithm == null || scatterGather == null) {
      throw new IllegalArgumentException("both routing algorithm and scattergather handler must be supplied");
    }
    
    if (shards == null) {
      shards = routingAlgorithm.getShards();
    }
    
    return new Service<Req, Res>(){
      @Override
      public Future<Res> apply(Req req) {
        Map<Integer, Future<Res>> futureList = new HashMap<Integer,Future<Res>>();
        for (Integer shard : shards) {
          Service<Req, Res> node = routingAlgorithm.route(routingKey, shard);
          if (node != null) {
            Req rewrittenReq = scatterGather.rewrite(req, shard);
            futureList.put(shard, node.apply(rewrittenReq));
          }
        }
        
        Map<Integer, Res> resList = new HashMap<Integer,Res>();
        
        for (Entry<Integer, Future<Res>> entry : futureList.entrySet()) {
          Res result = entry.getValue().apply(partialResultTimeout);
          resList.put(entry.getKey(), result);
        }
        
        return scatterGather.merge(resList);
      }
    }; 
  }
}
