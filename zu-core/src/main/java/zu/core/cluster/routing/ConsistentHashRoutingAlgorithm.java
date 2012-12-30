package zu.core.cluster.routing;

import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Consistent hash routing implementation based on:
 * http://weblogs.java.net/blog/tomwhite/archive/2007/11/consistent_hash.html
 */
public class ConsistentHashRoutingAlgorithm implements RoutingAlgorithm {

  private static class MD5HashProvider{
    
    private final ThreadLocal<MessageDigest> _md = new ThreadLocal<MessageDigest>()
        {
          protected MessageDigest initialValue()
          {
            try
            {
              return MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e)
            {
              throw new IllegalStateException(e.getMessage(),e);
            }
          }
        };

        public long hash(byte[] key)
        {
          byte[] kbytes = _md.get().digest(key);
          long hc = ((long) (kbytes[3] & 0xFF) << 24) | ((long) (kbytes[2] & 0xFF) << 16) | ((long) (kbytes[1] & 0xFF) << 8) | (long) (kbytes[0] & 0xFF);
          _md.get().reset();
          return Math.abs(hc);
        }
  }
  
  final private MD5HashProvider hashProvider = new MD5HashProvider();
  final private int numberOfReplicas;
  private volatile Map<Integer,TreeMap<Integer, InetSocketAddress>> circleMap = new HashMap<Integer,TreeMap<Integer, InetSocketAddress>>();
  
  public static final int DEFAULT_NUM_REPLICA = 50;
  
  public ConsistentHashRoutingAlgorithm(int numberOfReplicas){
    this.numberOfReplicas = numberOfReplicas;
  }
  
  public ConsistentHashRoutingAlgorithm(){
    this(DEFAULT_NUM_REPLICA);
  }

  @Override
  public void clusterChanged(
      Map<Integer, ArrayList<InetSocketAddress>> clusterView) {
    Map<Integer,TreeMap<Integer, InetSocketAddress>> tmpMap = new HashMap<Integer,TreeMap<Integer, InetSocketAddress>>();
    
    for (Entry<Integer,ArrayList<InetSocketAddress>> entry : clusterView.entrySet()){
      Integer partition = entry.getKey();
      TreeMap<Integer, InetSocketAddress> circle = new TreeMap<Integer, InetSocketAddress>();
      tmpMap.put(partition, circle);
      for (InetSocketAddress newNode : entry.getValue()){
        for (int count = 0; count < numberOfReplicas; ++count){
          String key = newNode.toString() + count;
          circle.put((int)hashProvider.hash(key.getBytes()), newNode);
        }
      }
    }
    circleMap = tmpMap;
  }

  @Override
  public void nodesRemovedFromCluster(List<InetSocketAddress> nodes) {
    
  }

  @Override
  public InetSocketAddress route(byte[] key, int partition) {
    TreeMap<Integer, InetSocketAddress> circle = circleMap.get(partition);
    if (circle == null || circle.isEmpty()){
      return null;
    }
    int hash = (int)hashProvider.hash(key);
    if (!circle.containsKey(hash)) {
      SortedMap<Integer, InetSocketAddress> tailMap = circle.tailMap(hash);
      hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
    }
    return circle.get(hash);
  }
}

