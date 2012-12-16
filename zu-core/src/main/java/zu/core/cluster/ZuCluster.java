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
import com.twitter.thrift.Status;

public class ZuCluster<S extends ZuService> implements HostChangeMonitor<ServiceInstance>{
  private static final int DEFAULT_TIMEOUT = 300;
  private final ServerSet serverSet;
  private final List<ZuClusterEventListener> lsnrs;
  private final PartitionInfoReader partitionReader;
  
  private static class NodeClusterView<S extends ZuService>{
    Map<Endpoint,InetSocketAddress> nodesMap = new HashMap<Endpoint,InetSocketAddress>();
    Map<Integer,ArrayList<InetSocketAddress>> partMap = new HashMap<Integer,ArrayList<InetSocketAddress>>();
  }
  
  private AtomicReference<NodeClusterView<S>> clusterView = new AtomicReference<NodeClusterView<S>>(new NodeClusterView<S>());

  public ZuCluster(String host, int port, PartitionInfoReader partitionReader, String clusterName) throws MonitorException {
    this(new InetSocketAddress(host,port), partitionReader, clusterName, DEFAULT_TIMEOUT);
  }
  
  public ZuCluster(String host, int port, PartitionInfoReader partitionReader, String clusterName,
      int timeout) throws MonitorException {
    this(new InetSocketAddress(host,port), partitionReader, clusterName, timeout);
  }
  
  public ZuCluster(InetSocketAddress zookeeperAddr, PartitionInfoReader partitionReader, String clusterName) throws MonitorException{
    this(zookeeperAddr, partitionReader, clusterName, DEFAULT_TIMEOUT);
  }
  
  public ZuCluster(InetSocketAddress zookeeperAddr, PartitionInfoReader partitionReader, String clusterName,
      int timeout) throws MonitorException{
    assert zookeeperAddr != null;
    assert clusterName != null;
    assert partitionReader != null;
    this.partitionReader = partitionReader;
    lsnrs = Collections.synchronizedList(new LinkedList<ZuClusterEventListener>());
    ZooKeeperClient zclient = new ZooKeeperClient(Amount.of(timeout,
        Time.SECONDS), Credentials.NONE, zookeeperAddr);
    
    if (!clusterName.startsWith("/")){
      clusterName = "/" + clusterName;
    }
    serverSet = new ServerSetImpl(zclient, clusterName);
    serverSet.monitor(this);
  }
  
  public void addClusterEventListener(ZuClusterEventListener lsnr){
    lsnrs.add(lsnr);
  }

  public EndpointStatus join(InetSocketAddress addr) throws JoinException, InterruptedException {
    return serverSet.join(addr, Collections.<String, InetSocketAddress>emptyMap(), Status.ALIVE);
  }
  
  public void leave(EndpointStatus status) throws UpdateException{
    status.update(Status.DEAD);
  }


  @Override
  public void onChange(ImmutableSet<ServiceInstance> hostSet) {
    NodeClusterView<S> oldView = clusterView.get();
    NodeClusterView<S> newView = new NodeClusterView<S>();
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
      List<Integer> parts = partitionReader.getPartitionFor(sa);
      for (Integer part : parts){
        ArrayList<InetSocketAddress> nodeList = newView.partMap.get(part);
        if (nodeList == null){
          nodeList = new ArrayList<InetSocketAddress>();
          newView.partMap.put(part, nodeList);
        }
        nodeList.add(svc);
      }
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

}
