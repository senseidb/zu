package zu.core.test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import org.junit.BeforeClass;
import org.junit.Test;

import zu.core.cluster.routing.ConsistentHashRoutingAlgorithm;
import zu.core.cluster.routing.RoutingAlgorithm;

public class ZuRoutingTest {

  private static ArrayList<InetSocketAddress> NodeList = new ArrayList<InetSocketAddress>();
  private static Map<Integer,ArrayList<InetSocketAddress>> ClusterView;
  @BeforeClass
  public static void init(){
    NodeList.add(new InetSocketAddress(1));
    NodeList.add(new InetSocketAddress(2));
    NodeList.add(new InetSocketAddress(3));
    ClusterView = new HashMap<Integer,ArrayList<InetSocketAddress>>();
    ClusterView.put(1, NodeList);
  }
  
  @Test
  public void testRandomRouting(){
    RoutingAlgorithm alg = RoutingAlgorithm.Random;
    alg.clusterChanged(ClusterView);
    Map<Integer,AtomicInteger> ids = new HashMap<Integer,AtomicInteger>();
    int numIter = 50;
    for (int i=0;i<numIter;++i){
      InetSocketAddress addr = alg.route(null, 1);
      int port = addr.getPort();
      AtomicInteger count = ids.get(port);
      if (count == null){
        count = new AtomicInteger(1);
        ids.put(port, count);
      }
      else{
        count.incrementAndGet();
      }
    }
    TestCase.assertTrue(ids.keySet().contains(1));
    TestCase.assertTrue(ids.keySet().contains(2));
    TestCase.assertTrue(ids.keySet().contains(3));
    
    int sum = 0;
    for (AtomicInteger count : ids.values()){
      sum+=count.get();
    }
    TestCase.assertEquals(numIter, sum);
  }
  
  @Test
  public void testRoundRobinRouting(){
    RoutingAlgorithm alg = RoutingAlgorithm.RoundRobin;
    alg.clusterChanged(ClusterView);
    int numIter = 10;

    List<Integer> expectedList = new LinkedList<Integer>();
    for (int i=0;i<numIter;++i){
      expectedList.add(i % NodeList.size() + 1);
    }
    List<Integer> orderList = new LinkedList<Integer>();
    for (int i=0;i<numIter;++i){
      InetSocketAddress addr = alg.route(null, 1);
      orderList.add(addr.getPort());
    }
    TestCase.assertEquals(expectedList, orderList);
  }
  
  @Test
  public void testConsistentHash(){
    RoutingAlgorithm alg = new ConsistentHashRoutingAlgorithm();
    alg.clusterChanged(ClusterView);
    int numIter = 10;
    String key = "test";

    Set<Integer> portSet = new HashSet<Integer>();
    for (int i=0;i<numIter;++i){
      InetSocketAddress addr = alg.route(key.getBytes(), 1);
      portSet.add(addr.getPort());
    }
    TestCase.assertEquals(1, portSet.size());
  }
}
