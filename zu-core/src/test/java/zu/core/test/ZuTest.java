package zu.core.test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;

import org.junit.BeforeClass;
import org.junit.Test;

import zu.core.cluster.ZuCluster;
import zu.core.cluster.ZuClusterEventListener;

import com.twitter.common.zookeeper.ServerSet.EndpointStatus;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common.zookeeper.testing.BaseZooKeeperTest;


public class ZuTest extends BaseZooKeeperTest{
  
  @BeforeClass
  public static void init() throws Exception{
    
  }
  
  static void validate(Map<Integer,Set<Integer>> expected, Map<Integer,ArrayList<InetSocketAddress>> view){
    for (Entry<Integer,ArrayList<InetSocketAddress>> entry : view.entrySet()){
      Integer key = entry.getKey();
      ArrayList<InetSocketAddress> list = entry.getValue();
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
    ZuCluster mockCluster = new ZuCluster(zkClient, "/core/test1");
    
    InetSocketAddress s1 = new InetSocketAddress(1);
    
    final Map<Integer,Set<Integer>> answer = new HashMap<Integer,Set<Integer>>();
    
    answer.put(0, new HashSet<Integer>(Arrays.asList(1)));
    answer.put(1, new HashSet<Integer>(Arrays.asList(1)));
    
    
    
    final AtomicBoolean flag = new AtomicBoolean(false);
    
    mockCluster.addClusterEventListener(new ZuClusterEventListener() {
      
      @Override
      public void clusterChanged(Map<Integer, ArrayList<InetSocketAddress>> clusterView) {
        int numPartsJoined = clusterView.size();
        System.out.println("num part joined: "+numPartsJoined);
        if (numPartsJoined == 2){
          validate(answer,clusterView);
          flag.set(true);
        }
      }

      @Override
      public void nodesRemovedFromCluster(List<InetSocketAddress> nodes) {
        
      }
    });

   
    List<EndpointStatus> e1 = mockCluster.join(s1, ZuTestUtil.CLUSTER_VIEW.get(s1.getPort()));
    
    while(!flag.get()){
      Thread.sleep(10);
    }
    
    mockCluster.leave(e1);
  }
  
  @Test
  public void testAllNodesJoined() throws Exception{
    
    ZooKeeperClient zkClient = createZkClient();
    ZuCluster mockCluster = new ZuCluster(zkClient, "/core/test2");
    
    InetSocketAddress s1 = new InetSocketAddress(1);
    InetSocketAddress s2 = new InetSocketAddress(2);
    InetSocketAddress s3 = new InetSocketAddress(3);
    
    final Map<Integer,Set<Integer>> answer = new HashMap<Integer,Set<Integer>>();
    
    answer.put(0, new HashSet<Integer>(Arrays.asList(1)));
    answer.put(1, new HashSet<Integer>(Arrays.asList(1,2)));
    answer.put(2, new HashSet<Integer>(Arrays.asList(2,3)));
    answer.put(3, new HashSet<Integer>(Arrays.asList(3)));
    
    final AtomicBoolean flag = new AtomicBoolean(false);
    
    mockCluster.addClusterEventListener(new ZuClusterEventListener() {  
      @Override
      public void clusterChanged(Map<Integer, ArrayList<InetSocketAddress>> clusterView) {
        int numPartsJoined = clusterView.size();
        if (numPartsJoined == 4){
          validate(answer,clusterView);
          flag.set(true);
        }
      }

      @Override
      public void nodesRemovedFromCluster(List<InetSocketAddress> nodes) {
        
      }
    });

   
    List<EndpointStatus> e1 = mockCluster.join(s1, ZuTestUtil.CLUSTER_VIEW.get(s1.getPort()));
    List<EndpointStatus> e2 = mockCluster.join(s2, ZuTestUtil.CLUSTER_VIEW.get(s2.getPort()));
    List<EndpointStatus> e3 = mockCluster.join(s3, ZuTestUtil.CLUSTER_VIEW.get(s3.getPort()));
    
    while(!flag.get()){
      Thread.sleep(10);
    }
    
    mockCluster.leave(e1);
    mockCluster.leave(e2);
    mockCluster.leave(e3);
  }
}
