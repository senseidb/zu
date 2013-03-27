package zu.core.cluster;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableSet;
import com.twitter.common.net.pool.DynamicHostSet.HostChangeMonitor;
import com.twitter.common.net.pool.DynamicHostSet.MonitorException;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.zookeeper.Group.JoinException;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.common.zookeeper.ServerSet.EndpointStatus;
import com.twitter.common.zookeeper.ServerSet.UpdateException;
import com.twitter.common.zookeeper.ServerSetImpl;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common.zookeeper.ZooKeeperClient.Credentials;
import com.twitter.thrift.Endpoint;
import com.twitter.thrift.ServiceInstance;

/**
 * A cluster abstraction.
 */
public class ZuCluster implements HostChangeMonitor<ServiceInstance>{
  /**
   * Default zookeeper timeout: 5 minutes.
   */
  public static final int DEFAULT_TIMEOUT = 300;
  private final ServerSet serverSet;
  private final ZooKeeperClient zkClient;
  private final List<ZuClusterEventListener> lsnrs;
  private final String clusterName;
  
  private static class NodeClusterView{
    Map<Endpoint,InetSocketAddress> nodesMap = new HashMap<Endpoint,InetSocketAddress>();
    Map<Integer,List<InetSocketAddress>> partMap = new HashMap<Integer,List<InetSocketAddress>>();
  }
  
  private AtomicReference<NodeClusterView> clusterView = new AtomicReference<NodeClusterView>(new NodeClusterView());

  /**
   * @param host zookeeper host
   * @param port zookeeper port
   * @param clusterName name of the cluster
   * @throws MonitorException
   */
  public ZuCluster(String host, int port, String clusterName) throws MonitorException {
    this(new InetSocketAddress(host,port), clusterName, DEFAULT_TIMEOUT);
  }
  
  /**
   * @param host zookeeper host
   * @param port zookeeper port
   * @param clusterName name of the cluster
   * @param timeout zookeeper timeout in seconds
   * @throws MonitorException
   */
  public ZuCluster(String host, int port, String clusterName,
      int timeout) throws MonitorException {
    this(new InetSocketAddress(host,port), clusterName, timeout);
  }
  
  /**
   * @param zookeeperAddr zookeeper address
   * @param clusterName name of the cluster
   * @throws MonitorException
   */
  public ZuCluster(InetSocketAddress zookeeperAddr, String clusterName) throws MonitorException{
    this(zookeeperAddr, clusterName, DEFAULT_TIMEOUT);
  }
  
  /**
   * @param zookeeperAddr zookeeper address
   * @param clusterName name of the cluster
   * @param timeout zookeeper timeout in seconds
   * @throws MonitorException
   */
  public ZuCluster(InetSocketAddress zookeeperAddr, String clusterName,
      int timeout) throws MonitorException{
    this(new ZooKeeperClient(Amount.of(timeout, Time.SECONDS), Credentials.NONE, zookeeperAddr), clusterName);
  }
  
  /**
   * @param zkClient A zookeeper client
   * @param clusterName name of the cluster
   * @throws MonitorException
   */
  public ZuCluster(ZooKeeperClient zkClient, String clusterName) throws MonitorException{
    assert zkClient != null;
    assert clusterName != null;
    lsnrs = Collections.synchronizedList(new LinkedList<ZuClusterEventListener>());
    
    if (!clusterName.startsWith("/")){
      clusterName = "/" + clusterName;
    }
    
    this.clusterName = clusterName;
    this.zkClient = zkClient;
    serverSet = new ServerSetImpl(zkClient, clusterName);
    serverSet.monitor(this);
  }
  
  public String getClusterName() {
    return clusterName;
  }
  
  /**
   * Adds a listener for cluster events
   * @param lsnr cluster listener
   */
  public void addClusterEventListener(ZuClusterEventListener lsnr){
    lsnrs.add(lsnr);
  }

  /**
   * joins the cluster
   * @param addr node address
   * @param shards list of paritions ids this node supports
   * @return a list of handles, one for each partition
   * @throws JoinException
   * @throws InterruptedException
   */
  public List<EndpointStatus> join(InetSocketAddress addr, Set<Integer> shards) throws JoinException, InterruptedException {
    ArrayList<EndpointStatus> statuses = new ArrayList<EndpointStatus>(shards.size());
    for (Integer shard : shards){
      try{
        statuses.add(serverSet.join(addr, Collections.<String, InetSocketAddress>emptyMap(), shard));
      }
      catch(JoinException je){
        
        // remove dirty state
        try{
          leave(statuses);
        }
        catch(UpdateException ue){
          // ignore
        }
        throw je;
      }
      catch(InterruptedException ie){
     // remove dirty state
        try{
          leave(statuses);
        }
        catch(UpdateException ue){
          // ignore
        }
        throw ie;
      }
    }
    return statuses;
  }
  
  /**
   * leaves the cluster
   * @param statuses list of handles from joining the cluster
   * @throws UpdateException
   */
  public void leave(List<EndpointStatus> statuses) throws UpdateException{
    UpdateException ex = null;
    for (EndpointStatus status : statuses){
      try{
        status.leave();
      }
      catch(UpdateException ue){
        ex = ue;
      }
    }
    if (ex != null){
      throw ex;
    }
  }


  @Override
  public void onChange(ImmutableSet<ServiceInstance> hostSet) {
    NodeClusterView oldView = clusterView.get();
    NodeClusterView newView = new NodeClusterView();
    List<InetSocketAddress> cleanupList = new LinkedList<InetSocketAddress>();
    
    for (ServiceInstance si : hostSet){
      
      Endpoint ep = si.getServiceEndpoint();

      InetSocketAddress svc = oldView.nodesMap.get(ep);
      InetSocketAddress sa = new InetSocketAddress(ep.getHost(), ep.getPort());
      if (svc == null){
        // discovered a new node
        svc = sa;
      }
      newView.nodesMap.put(ep, svc);
      int shardId = si.getShard();
      List<InetSocketAddress> nodeList = newView.partMap.get(shardId);
      if (nodeList == null){
        nodeList = new ArrayList<InetSocketAddress>();
        newView.partMap.put(shardId, nodeList);
      }
      nodeList.add(svc);
    }
    
 // gather a list of clients that are no longer in the cluster and cleanup
    Set<Entry<Endpoint,InetSocketAddress>> entries = oldView.nodesMap.entrySet();
    Set<Endpoint> newEndpoints = newView.nodesMap.keySet();
    for (Entry<Endpoint,InetSocketAddress> entry : entries){
      if (!newEndpoints.contains(entry.getKey())){
        cleanupList.add(entry.getValue());
      }
    }

    clusterView.set(newView);
    
    for (ZuClusterEventListener lsnr : lsnrs){
     lsnr.clusterChanged(newView.partMap); 
    }
  }
  
  /**
   * shuts down the cluster and closes connection to zookeeper
   */
  public void shutdown() {
    if (zkClient != null) {
      zkClient.close();
    }
  }

}
