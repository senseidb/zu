package zu.core.cluster.routing;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Consistent hash routing implementation based on:
 * http://weblogs.java.net/blog/tomwhite/archive/2007/11/consistent_hash.html
 */
public class ConsistentHashRoutingAlgorithm<T> extends RoutingAlgorithm<T> {

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
  private volatile Map<Integer,TreeMap<Integer, T>> circleMap = new HashMap<Integer,TreeMap<Integer, T>>();
  
  public static final int DEFAULT_NUM_REPLICA = 50;
  
  public ConsistentHashRoutingAlgorithm(int numberOfReplicas, InetSocketAddressDecorator<T> socketDecorator){
    super(socketDecorator);
    this.numberOfReplicas = numberOfReplicas;
  }
  
  public ConsistentHashRoutingAlgorithm(InetSocketAddressDecorator<T> socketDecorator){
    this(DEFAULT_NUM_REPLICA, socketDecorator);
  }

  @Override
  public void updateCluster(
      Map<Integer, ArrayList<T>> clusterView) {
    Map<Integer,TreeMap<Integer, T>> tmpMap = new HashMap<Integer,TreeMap<Integer, T>>();
    
    for (Entry<Integer,ArrayList<T>> entry : clusterView.entrySet()){
      Integer partition = entry.getKey();
      TreeMap<Integer, T> circle = new TreeMap<Integer, T>();
      tmpMap.put(partition, circle);
      for (T newNode : entry.getValue()){
        for (int count = 0; count < numberOfReplicas; ++count){
          String key = newNode.toString() + count;
          circle.put((int)hashProvider.hash(key.getBytes()), newNode);
        }
      }
    }
    circleMap = tmpMap;
  }


  @Override
  public T route(byte[] key, int partition) {
    TreeMap<Integer, T> circle = circleMap.get(partition);
    if (circle == null || circle.isEmpty()){
      return null;
    }
    int hash = (int)hashProvider.hash(key);
    if (!circle.containsKey(hash)) {
      SortedMap<Integer, T> tailMap = circle.tailMap(hash);
      hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
    }
    return circle.get(hash);
  }
}

