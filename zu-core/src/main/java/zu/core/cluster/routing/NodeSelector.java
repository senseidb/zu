package zu.core.cluster.routing;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class NodeSelector {

  public static Map<Integer, InetSocketAddress> selectNodes(byte[] key, Map<Integer, ArrayList<InetSocketAddress>> clusterView, 
      RoutingAlgorithm routingAlg){
    return selectNodes(key, null,  clusterView, routingAlg);
  }
  
  public static Map<Integer, InetSocketAddress> selectNodes(byte[] key, Set<Integer> partitions,
      Map<Integer, ArrayList<InetSocketAddress>> clusterView, 
      RoutingAlgorithm routingAlg){
    Iterator<Integer> partIter = partitions == null ? clusterView.keySet().iterator() : partitions.iterator();
    HashMap<Integer,InetSocketAddress> selectedNodes = new HashMap<Integer,InetSocketAddress>();
    
    while(partIter.hasNext()){
      Integer part = partIter.next();
      ArrayList<InetSocketAddress> nodeList = clusterView.get(part);
      InetSocketAddress node = routingAlg.route(key, part, nodeList);
      selectedNodes.put(part, node);
    }
    
    return selectedNodes;
  }
}
