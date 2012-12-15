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
  private final List<ZuClusterEventListener<S>> lsnrs;
  private final ZuServiceFactory<S> svcFactory;
  private final PartitionInfoReader partitionReader;
  private volatile boolean monitored = false;
  
  private static class NodeClusterView<S extends ZuService>{
    Map<Endpoint,S> nodesMap = new HashMap<Endpoint,S>();
    Map<Integer,ArrayList<S>> partMap = new HashMap<Integer,ArrayList<S>>();
  }
  
  private AtomicReference<NodeClusterView<S>> clusterView = new AtomicReference<NodeClusterView<S>>(new NodeClusterView<S>());

  public ZuCluster(String host, int port, PartitionInfoReader partitionReader, ZuServiceFactory<S> svcFactory, String clusterName) throws MonitorException {
    this(new InetSocketAddress(host,port), partitionReader, svcFactory, clusterName, DEFAULT_TIMEOUT);
  }
  
  public ZuCluster(String host, int port, PartitionInfoReader partitionReader, ZuServiceFactory<S> svcFactory, String clusterName,
      int timeout) throws MonitorException {
    this(new InetSocketAddress(host,port), partitionReader, svcFactory, clusterName, timeout);
  }
  
  public ZuCluster(InetSocketAddress zookeeperAddr, PartitionInfoReader partitionReader, ZuServiceFactory<S> svcFactory, String clusterName) throws MonitorException{
    this(zookeeperAddr, partitionReader, svcFactory, clusterName, DEFAULT_TIMEOUT);
  }
  
  public ZuCluster(InetSocketAddress zookeeperAddr, PartitionInfoReader partitionReader, ZuServiceFactory<S> svcFactory, String clusterName,
      int timeout) throws MonitorException{
    assert svcFactory != null;
    assert zookeeperAddr != null;
    assert clusterName != null;
    assert partitionReader != null;
    this.partitionReader = partitionReader;
    this.svcFactory = svcFactory;
    lsnrs = Collections.synchronizedList(new LinkedList<ZuClusterEventListener<S>>());
    ZooKeeperClient zclient = new ZooKeeperClient(Amount.of(timeout,
        Time.SECONDS), Credentials.NONE, zookeeperAddr);
    
    serverSet = new ServerSetImpl(zclient, clusterName);
    serverSet.monitor(this);
  }
  
  public void addClusterEventListener(ZuClusterEventListener<S> lsnr){
    lsnrs.add(lsnr);
  }

  public EndpointStatus join(S svc) throws JoinException, InterruptedException {
    return serverSet.join(svc.getAddress(), Collections.EMPTY_MAP, Status.ALIVE);
  }
  
  public void leave(EndpointStatus status) throws UpdateException{
    status.update(Status.DEAD);
  }


  @Override
  public void onChange(ImmutableSet<ServiceInstance> hostSet) {
    NodeClusterView<S> oldView = clusterView.get();
    NodeClusterView<S> newView = new NodeClusterView<S>();
    List<S> cleanupList = new LinkedList<S>();
    
    for (ServiceInstance si : hostSet){
      Endpoint ep = si.getServiceEndpoint();

      S svc = oldView.nodesMap.get(ep);
      InetSocketAddress sa = new InetSocketAddress(ep.getHost(), ep.getPort());
      if (svc == null){
        // discovered a new node
        svc = svcFactory.getService(sa);
      }
      newView.nodesMap.put(ep, svc);
      List<Integer> parts = partitionReader.getPartitionFor(sa);
      for (Integer part : parts){
        ArrayList<S> nodeList = newView.partMap.get(part);
        if (nodeList == null){
          nodeList = new ArrayList<S>();
          newView.partMap.put(part, nodeList);
        }
        nodeList.add(svc);
      }
    }
    
 // gather a list of clients that are no longer in the cluster and cleanup
    Set<Entry<Endpoint,S>> entries = oldView.nodesMap.entrySet();
    Set<Endpoint> newEndpoints = newView.nodesMap.keySet();
    for (Entry<Endpoint,S> entry : entries){
      if (!newEndpoints.contains(entry.getKey())){
        cleanupList.add(entry.getValue());
      }
    }

    clusterView.set(newView);
    
    for (ZuClusterEventListener<S> lsnr : lsnrs){
     lsnr.clusterChanged(newView.partMap); 
    }
    
    for (S client : cleanupList){
      client.shutdown();
    }
  }

}
