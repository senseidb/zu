package zu.core.cluster;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.ImmutableSet;

public class MockCluster implements Cluster {

  private final String namespace;
  private final String id;
  
  private final Map<Integer, List<InetSocketAddress>> clusterMap = new ConcurrentHashMap<>();
  private final List<ZuClusterEventListener> listeners = new LinkedList<>();
  
  public MockCluster(String namespace, String id) {
    this.id = id;
    this.namespace = namespace;
  }
  
  @Override
  public String id() {
    return id;
  }

  @Override
  public String namespace() {
    return namespace;
  }

  @Override
  public Map<Integer, List<InetSocketAddress>> getClusterView() {
    return Collections.unmodifiableMap(clusterMap);
  }

  @Override
  public void addClusterEventListener(ZuClusterEventListener lsnr) {
    listeners.add(lsnr);
  }

  @Override
  public Membership join(final InetSocketAddress addr, Set<Integer> shards)
      throws Exception {
    for (Integer shard : shards) {
      List<InetSocketAddress> addrList = clusterMap.get(shard);
      if (addrList == null) {
        addrList = new LinkedList<>();        
        clusterMap.put(shard, addrList);
      }
      addrList.add(addr);
    }
    
    for (ZuClusterEventListener lsnr : listeners) {
      lsnr.clusterChanged(Collections.unmodifiableMap(clusterMap));
    }
    return new Membership() {
      @Override
      public void leave() throws Exception {
        for (List<InetSocketAddress> addrList : clusterMap.values()) {
          addrList.remove(addr);          
        }
        for (ZuClusterEventListener lsnr : listeners) {
          lsnr.nodesRemoved(ImmutableSet.of(addr));
        }
      }
    };
  }

  @Override
  public void shutdown() {    
  }

}
