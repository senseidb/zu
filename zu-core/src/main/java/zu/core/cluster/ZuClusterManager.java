package zu.core.cluster;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.twitter.common.zookeeper.ZooKeeperClient;

public class ZuClusterManager implements Watcher {

  private static final Logger logger = LoggerFactory.getLogger(ZookeeperClientBuilder.class);
  
  private final ZooKeeperClient zkClient;
  private AtomicReference<Set<String>> clusters;
  private final String clusterPrefix;
  private final Map<String, ZuCluster> clusterMap = Collections.synchronizedMap(Maps.newHashMap());
  private boolean closeOnShutdown;
  
  public ZuClusterManager(String clusterUrl, String clusterPrefix) throws Exception {    
    this(new ZookeeperClientBuilder().setZookeeperUrl(clusterUrl).build(), clusterPrefix, true);
  }
  
  public ZuClusterManager(ZooKeeperClient zkClient, String clusterPrefix, boolean closeOnShutdown) throws Exception {
    this.clusterPrefix = clusterPrefix.startsWith("/") ? clusterPrefix : "/" + clusterPrefix;    
    this.zkClient = zkClient;
    clusters = new AtomicReference<>();    
    // if prefix does not exist, create it
    try {
      ZooKeeper zk = zkClient.get();
      if (zk.exists(this.clusterPrefix, false) == null) {
        logger.info(this.clusterPrefix + " does not exist, creating a persistent node");
        zk.create(this.clusterPrefix, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
    } catch(Exception e) {
      logger.error(e == null ? "cannot create prefix path" : e.getMessage());
    }
    zkClient.register(this);
    watchForClusters();
    this.closeOnShutdown = closeOnShutdown;
  }
  
  public Set<String> getAvailableClusters() {
    return clusters.get();
  }
  
  private void watchForClusters() throws Exception {    
    Set<String> clusterSet = ZookeeperClientBuilder.getAvailableClusters(zkClient, clusterPrefix, true);
    // remove cluster new longer applicable
    Set<String> tobeRemoved = Sets.newHashSet();
    for (String cluster : clusterMap.keySet()) {
      if (!clusterSet.contains(cluster)) {
        clusterSet.add(cluster);
      }
    }
    
    for(String c : tobeRemoved) {
      ZuCluster zuCluster = clusterMap.remove(c);
      zuCluster.shutdown();
    }
    
    for (String cluster : clusterSet) {
      if (!clusterMap.containsKey(cluster)) {
        ZuCluster zuCluster = new ZuCluster(zkClient, clusterPrefix, cluster, false);
        clusterMap.put(cluster, zuCluster);
      }
    }
    
    clusters.set(clusterSet);
    logger.info("updated cluster list: " + clusters.get());
  }
  
  public ZuCluster getCluster(String clusterName) {
    return clusterName != null ? clusterMap.get(clusterName) : null;
  }
  
  @Override
  public void process(WatchedEvent event) {
    try {
      watchForClusters();
    } catch(Exception e) {
      logger.error(e == null ? "problem watching for cluster changes" : e.getMessage());
    }
  }
  
  public void shutdown() {
    if (zkClient != null) {
      zkClient.unregister(this);
      if (closeOnShutdown) {
        zkClient.close();
      }
    }
  }
  
  public static void main(String[] args)  throws Exception {
    ZuClusterManager clusterManager = new ZuClusterManager("localhost:2181", "/dashbase/cluster");
    clusterManager.shutdown();
  }
}