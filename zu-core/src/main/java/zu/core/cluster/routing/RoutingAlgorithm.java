package zu.core.cluster.routing;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public interface RoutingAlgorithm {
  InetSocketAddress route(byte[] key, int partition, ArrayList<InetSocketAddress> nodes);
  
  public static final RoutingAlgorithm Random = new RandomAlgorithm();
  public static final RoutingAlgorithm RoundRobin = new RoundRobinAlgorithm();

  public static class RandomAlgorithm implements RoutingAlgorithm {
    private Random rand = new Random();
    
    @Override
    public InetSocketAddress route(byte[] key, int partition,
        ArrayList<InetSocketAddress> nodes) {
      return nodes.get(rand.nextInt(nodes.size()));
    }
  }
  
  public static class RoundRobinAlgorithm implements RoutingAlgorithm {
    private final Map<Integer,AtomicLong> countMap = Collections.synchronizedMap(new HashMap<Integer,AtomicLong>());
    @Override
    public InetSocketAddress route(byte[] key, int partition,
        ArrayList<InetSocketAddress> nodes) {
      AtomicLong idx = countMap.get(partition);
      long idxVal = 0;
      if (idx == null){
        idx = new AtomicLong(0);
        countMap.put(partition, idx);
      }
      else{
        idxVal = idx.get();
      }
      return nodes.get((int)((long)nodes.size()%idxVal));
    }
  }
}
