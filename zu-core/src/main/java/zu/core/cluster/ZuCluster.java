package zu.core.cluster;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

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
  private final List<ZuClusterEventListener> listeners;
  private final String clusterId;
  private final String prefix;
  private final ZooKeeperClient zkClient;
  private final boolean closeOnShutdown;
  private final Logger logger = Logger.getLogger(ZuCluster.class);
  
  private static class NodeClusterView{
    Map<Endpoint,InetSocketAddress> nodesMap = new HashMap<Endpoint,InetSocketAddress>();
    Map<Integer,List<InetSocketAddress>> partMap = new HashMap<Integer,List<InetSocketAddress>>();
  }
  
  private AtomicReference<NodeClusterView> clusterView = new AtomicReference<NodeClusterView>(new NodeClusterView());

  /**
   * @param host zookeeper host
   * @param port zookeeper port
   * @param clusterId name of the cluster
   * @throws MonitorException
   */
  public ZuCluster(String host, int port, String prefix, String clusterId) throws MonitorException {
    this(Arrays.asList(new InetSocketAddress(host,port)), prefix, clusterId, DEFAULT_TIMEOUT);
  }
  
  /**
   * @param host zookeeper host
   * @param port zookeeper port
   * @param clusterId name of the cluster
   * @param timeout zookeeper timeout in seconds
   * @throws MonitorException
   */
  public ZuCluster(String host, int port, String prefix, String clusterId,
      int timeout) throws MonitorException {
    this(Arrays.asList(new InetSocketAddress(host,port)), prefix, clusterId, timeout);
  }
  
  /**
   * @param zookeeperAddrs zookeeper hosts
   * @param clusterId name of the cluster
   * @param timeout zookeeper timeout in seconds
   * @throws MonitorException
   */
  public ZuCluster(Iterable<InetSocketAddress> zookeeperAddrs, String prefix, String clusterId,
      int timeout) throws MonitorException{
    this(new ZooKeeperClient(Amount.of(timeout, Time.SECONDS), Credentials.NONE, zookeeperAddrs), prefix, clusterId, true);
  }
  
  /**
   * @param zkClient A zookeeper client
   * @param clusterId name of the cluster
   * @throws MonitorException
   */
  public ZuCluster(ZooKeeperClient zkClient, String prefix, String clusterId, boolean closeOnShutdown) throws MonitorException{
    assert zkClient != null;
    assert clusterId != null;
    listeners = new LinkedList<ZuClusterEventListener>();
    
    this.prefix = "/" + StringUtils.strip(prefix);
    this.clusterId = StringUtils.strip(clusterId);
    this.zkClient = zkClient;   
    this.closeOnShutdown = closeOnShutdown;
    serverSet = new ServerSetImpl(zkClient, this.prefix + "/" + this.clusterId);
    serverSet.monitor(this);
  }
  
  public String getClusterPrefix() {
    return prefix;
  }
  
  public String getClusterId() {
    return clusterId;
  }
  
  /**
   * Adds a listener for cluster events
   * @param lsnr cluster listener
   */
  public void addClusterEventListener(ZuClusterEventListener lsnr){
    synchronized(listeners) {
      listeners.add(lsnr);
      lsnr.clusterChanged(this.clusterView.get().partMap);
    }
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
    Set<InetSocketAddress> cleanupList = new HashSet<InetSocketAddress>();
    
    for (ServiceInstance si : hostSet){
      
      Endpoint endpoint = si.getServiceEndpoint();

      InetSocketAddress host = oldView.nodesMap.get(endpoint);
      if (host == null){
        // discovered a new node
        host = new InetSocketAddress(endpoint.getHost(), endpoint.getPort());
      }
      newView.nodesMap.put(endpoint, host);
      int shardId = si.getShard();
      List<InetSocketAddress> nodeList = newView.partMap.get(shardId);
      if (nodeList == null){
        nodeList = new ArrayList<InetSocketAddress>();
        newView.partMap.put(shardId, nodeList);
      }
      nodeList.add(host);
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
    
    synchronized(listeners) {
      for (ZuClusterEventListener lsnr : listeners) {
        try {
          lsnr.clusterChanged(newView.partMap);
        } catch (Exception e) {
          logger.error("caught an exception while notifying a listener " + lsnr, e);
        }
      }

      for (ZuClusterEventListener lsnr : listeners) {
        try {
          lsnr.nodesRemoved(cleanupList);
        } catch (Exception e) {
          logger.error("caught an exception while notifying a listener " + lsnr, e);
        }
      }
    }
  }
  
  /**
   * shuts down the cluster and closes connection to zookeeper
   */
  public void shutdown() {    
    if (closeOnShutdown && zkClient != null) {
      zkClient.close();
    }
  }

  public Map<Integer,List<InetSocketAddress>> getClusterView() {
    NodeClusterView view = clusterView.get();
    if (view == null) {
      return Collections.emptyMap();
    }
    return view.partMap == null ? Collections.emptyMap() : view.partMap;
  }
}
