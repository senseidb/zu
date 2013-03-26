package zu.core.cluster.routing;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import zu.core.cluster.ZuClusterEventListener;

public abstract class RoutingAlgorithm<T> implements ZuClusterEventListener{
  protected volatile Map<Integer,ArrayList<T>> clusterView;
  private volatile Set<Integer> shards = null;
  private final InetSocketAddressDecorator<T> socketDecorator;
  
  public RoutingAlgorithm(InetSocketAddressDecorator<T> socketDecorator) {
    this.socketDecorator = socketDecorator;
  }
  
  public abstract T route(byte[] key, int partition);
  
  public Set<Integer> getShards() {
    return shards == null ? new HashSet<Integer>() : shards;
  }
  @Override
  public final void clusterChanged(Map<Integer, List<InetSocketAddress>> view){
    shards = view.keySet();
    Map<Integer, ArrayList<T>> clusterView = new HashMap<Integer, ArrayList<T>>();
    
    for (Entry<Integer,List<InetSocketAddress>> entry : view.entrySet()) {
      Integer key = entry.getKey();
      List<InetSocketAddress> value = entry.getValue();
      ArrayList<T> list = new ArrayList<T>(value.size());
      for (InetSocketAddress addr : value) {
        T elem = socketDecorator.decorate(addr);
        if (elem != null) {
          list.add(elem);
        }
      }
      clusterView.put(key, list);
    }
    
    Set<T> set = new HashSet<T>();
    Collection<ArrayList<T>> oldValues = clusterView.values();
    for (ArrayList<T> list : oldValues) {
      for (T t : list) {
        set.add(t);
      }
    }
    updateCluster(clusterView);
    socketDecorator.cleanup(set);
  }
  
  public void updateCluster(Map<Integer,ArrayList<T>> clusterView){
    this.clusterView = clusterView;
  }

  public static class RandomAlgorithm<T> extends RoutingAlgorithm<T> {
    private Random rand = new Random();
    
    
    public RandomAlgorithm(InetSocketAddressDecorator<T> socketDecorator){
      super(socketDecorator);
    }
    
    @Override
    public T route(byte[] key, int partition) {
      if (clusterView == null) return null;
      ArrayList<T> nodes = clusterView.get(partition);
      if (nodes.isEmpty()) return null;
      return nodes.get(rand.nextInt(nodes.size()));
    }
  }
  
  public static class RoundRobinAlgorithm<T> extends RoutingAlgorithm<T> {
    private final Map<Integer,AtomicLong> countMap = Collections.synchronizedMap(new HashMap<Integer,AtomicLong>());
    
    public RoundRobinAlgorithm(InetSocketAddressDecorator<T> socketDecorator){
      super(socketDecorator);
    }
    
    @Override
    public T route(byte[] key, int partition) {
      if (clusterView == null) {
        return null;
      }
      ArrayList<T> nodes = clusterView.get(partition);
      if (nodes.isEmpty()) return null;
      AtomicLong idx = countMap.get(partition);
      long idxVal = 0;
      if (idx == null){
        idx = new AtomicLong(0);
        countMap.put(partition, idx);
      }
      else{
        idxVal = idx.incrementAndGet();
      }
      return nodes.get((int)(idxVal % (long)nodes.size()));
    }
  }
}
