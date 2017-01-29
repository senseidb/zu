package zu.core.test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.twitter.common.zookeeper.ServerSet.EndpointStatus;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common.zookeeper.testing.BaseZooKeeperTest;

import junit.framework.Assert;
import junit.framework.TestCase;
import zu.core.cluster.ZuCluster;
import zu.core.cluster.ZuClusterEventListener;
import zu.core.cluster.ZuClusterManager;


public class ZuTest extends BaseZooKeeperTest{
  
  private static final Map<Integer,Set<Integer>> CLUSTER_VIEW;
  
  static{
    CLUSTER_VIEW = new HashMap<Integer,Set<Integer>>();
    CLUSTER_VIEW.put(1, new HashSet<Integer>(Arrays.asList(0,1)));
    CLUSTER_VIEW.put(2, new HashSet<Integer>(Arrays.asList(1,2)));
    CLUSTER_VIEW.put(3, new HashSet<Integer>(Arrays.asList(2,3)));
  }
  
  @BeforeClass
  public static void init() throws Exception{
    
  }
  
  static void validate(Map<Integer,Set<Integer>> expected, Map<Integer,List<InetSocketAddress>> view){
    for (Entry<Integer,List<InetSocketAddress>> entry : view.entrySet()){
      Integer key = entry.getKey();
      List<InetSocketAddress> list = entry.getValue();
      HashSet<Integer> ports = new HashSet<Integer>();
      for (InetSocketAddress svc : list){
        ports.add(svc.getPort());
      }
      Set<Integer> expectedSet = expected.remove(key);
      TestCase.assertNotNull(expectedSet);
      TestCase.assertEquals(expectedSet, ports);
    }
    TestCase.assertTrue(expected.isEmpty());
  }
  
  @Test
  public void testBasic() throws Exception{
    ZooKeeperClient zkClient = createZkClient();
    ZuCluster mockCluster = new ZuCluster(zkClient, "core", "test1", true);
    
    InetSocketAddress s1 = new InetSocketAddress(1);
    
    final Map<Integer,Set<Integer>> answer = new HashMap<Integer,Set<Integer>>();
    
    answer.put(0, new HashSet<Integer>(Arrays.asList(1)));
    answer.put(1, new HashSet<Integer>(Arrays.asList(1)));
    
    final CountDownLatch latch = new CountDownLatch(2);
    
    final CountDownLatch shutdownLatch = new CountDownLatch(1);
    
    mockCluster.addClusterEventListener(new ZuClusterEventListener() {
      
      @Override
      public void clusterChanged(Map<Integer, List<InetSocketAddress>> clusterView) {
        int numPartsJoined = clusterView.size();
        if (numPartsJoined == 2){
          validate(answer,clusterView);
          latch.countDown();
          latch.countDown();
        }
      }

      @Override
      public void nodesRemoved(Set<InetSocketAddress> removedNodes) {
        shutdownLatch.countDown();
      }
    });

   
    List<EndpointStatus> e1 = mockCluster.join(s1, CLUSTER_VIEW.get(s1.getPort()));
    
    latch.await();
    
    mockCluster.leave(e1);
    shutdownLatch.await();
  }
  
  @Test
  public void testAllNodesJoined() throws Exception{
    
    ZooKeeperClient zkClient = createZkClient();
    ZuCluster mockCluster = new ZuCluster(zkClient, "core", "test2", true);
    
    InetSocketAddress s1 = new InetSocketAddress(1);
    InetSocketAddress s2 = new InetSocketAddress(2);
    InetSocketAddress s3 = new InetSocketAddress(3);
    
    final Map<Integer,Set<Integer>> answer = new HashMap<Integer,Set<Integer>>();
    
    answer.put(0, new HashSet<Integer>(Arrays.asList(1)));
    answer.put(1, new HashSet<Integer>(Arrays.asList(1,2)));
    answer.put(2, new HashSet<Integer>(Arrays.asList(2,3)));
    answer.put(3, new HashSet<Integer>(Arrays.asList(3)));
    
    final HashSet<Integer> partitions = new HashSet<Integer>();
    
    final HashSet<InetSocketAddress> droppedServers = new HashSet<InetSocketAddress>();
    
    final CountDownLatch latch = new CountDownLatch(4);
    final CountDownLatch shutdownLatch = new CountDownLatch(3);
    mockCluster.addClusterEventListener(new ZuClusterEventListener() {  
      @Override
      public void clusterChanged(Map<Integer, List<InetSocketAddress>> clusterView) {
        int numPartsJoined = clusterView.size();
        if (!partitions.contains(numPartsJoined)) {
          partitions.add(numPartsJoined);
          latch.countDown();
        }
      }

      @Override
      public void nodesRemoved(Set<InetSocketAddress> removedNodes) {
        for (InetSocketAddress node : removedNodes) {
          if (!droppedServers.contains(node)) {
            droppedServers.add(node);
            shutdownLatch.countDown();
          }
        }
      }
    });

   
    List<EndpointStatus> e1 = mockCluster.join(s1, CLUSTER_VIEW.get(s1.getPort()));
    List<EndpointStatus> e2 = mockCluster.join(s2, CLUSTER_VIEW.get(s2.getPort()));
    List<EndpointStatus> e3 = mockCluster.join(s3, CLUSTER_VIEW.get(s3.getPort()));
    
    latch.await();
    
    mockCluster.leave(e1);
    mockCluster.leave(e2);
    mockCluster.leave(e3);
    
    shutdownLatch.await();
  }
  
  @Test
  public void testClusterManager() throws Exception {
    String prefix = "test";
    ZooKeeperClient zkClient = createZkClient();
    ZuClusterManager clusterManager = new ZuClusterManager(zkClient, prefix, false);
    
    Assert.assertEquals(0, clusterManager.getAvailableClusters().size());
    
    // join 1cluster
    final CountDownLatch latch = new CountDownLatch(1);
    ZuCluster mockCluster1 = new ZuCluster(zkClient, "test", "test1", false);
    mockCluster1.addClusterEventListener(new ZuClusterEventListener() {      
      @Override
      public void clusterChanged(
          Map<Integer, List<InetSocketAddress>> clusterView) {
        if (!clusterView.isEmpty()) {
          latch.countDown();
        }
      }
    });
    mockCluster1.join(new InetSocketAddress(1111), ImmutableSet.of(0));
    
    latch.await();
    Assert.assertEquals(1, clusterManager.getAvailableClusters().size());
    Assert.assertNotNull(clusterManager.getCluster("test1"));
    
    // join second cluster
    final CountDownLatch latch2 = new CountDownLatch(1);
    ZuCluster mockCluster2 = new ZuCluster(zkClient, "test", "test2", false);    
    mockCluster2.addClusterEventListener(new ZuClusterEventListener() {      
      @Override
      public void clusterChanged(
          Map<Integer, List<InetSocketAddress>> clusterView) {
        if (!clusterView.isEmpty()) {
          latch2.countDown();
        }
      }
    });
    mockCluster2.join(new InetSocketAddress(1111), ImmutableSet.of(0));
    latch2.await();
    Assert.assertEquals(2, clusterManager.getAvailableClusters().size());
    Assert.assertNotNull(clusterManager.getCluster("test2"));
    
    mockCluster1.shutdown();
    mockCluster2.shutdown();
    
    clusterManager.shutdown();
    zkClient.close();
  }
  
  @Test
  public void testClusterView() throws Exception {
    ZooKeeperClient zkClient = createZkClient();
    
    // join 1cluster
    final CountDownLatch latch = new CountDownLatch(1);
    ZuCluster mockCluster = new ZuCluster(zkClient, "test", "test", false);
    
    Map<Integer,List<InetSocketAddress>> view = mockCluster.getClusterView();
    
    Assert.assertTrue(view.isEmpty());
    
    mockCluster.addClusterEventListener(new ZuClusterEventListener() {      
      @Override
      public void clusterChanged(
          Map<Integer, List<InetSocketAddress>> clusterView) {
        if (clusterView.get(0) != null) {
          latch.countDown();
        }
      }
    });
    mockCluster.join(new InetSocketAddress(1111), ImmutableSet.of(0));
    
    latch.await();
    
    view = mockCluster.getClusterView();
    Assert.assertEquals(1, view.size());
    Assert.assertNotNull(view.get(0));
    
    // join second cluster
    final CountDownLatch latch2 = new CountDownLatch(1);    
    mockCluster.addClusterEventListener(new ZuClusterEventListener() {      
      @Override
      public void clusterChanged(
          Map<Integer, List<InetSocketAddress>> clusterView) {
        if (clusterView.get(1) != null) {
          latch2.countDown();
        }
      }
    });
    mockCluster.join(new InetSocketAddress(1111), ImmutableSet.of(1));
    latch2.await();
    
    view = mockCluster.getClusterView();
    Assert.assertEquals(2, view.size());
    Assert.assertNotNull(view.get(0));
    Assert.assertNotNull(view.get(1));
    
    mockCluster.shutdown();
    
    zkClient.close();
  }
}
