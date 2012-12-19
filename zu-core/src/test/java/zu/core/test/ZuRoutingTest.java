package zu.core.test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import org.junit.BeforeClass;
import org.junit.Test;

import zu.core.cluster.routing.RoutingAlgorithm;

public class ZuRoutingTest {

  private static ArrayList<InetSocketAddress> NodeList = new ArrayList<InetSocketAddress>();
  
  @BeforeClass
  public static void init(){
    NodeList.add(new InetSocketAddress(1));
    NodeList.add(new InetSocketAddress(2));
    NodeList.add(new InetSocketAddress(3));
  }
  
  @Test
  public void testRandomRouting(){
    RoutingAlgorithm alg = RoutingAlgorithm.Random;
    Map<Integer,AtomicInteger> ids = new HashMap<Integer,AtomicInteger>();
    int numIter = 10;
    for (int i=0;i<numIter;++i){
      InetSocketAddress addr = alg.route(null, 1, NodeList);
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
    int numIter = 10;

    List<Integer> expectedList = new LinkedList<Integer>();
    for (int i=0;i<numIter;++i){
      expectedList.add(i % NodeList.size() + 1);
    }
    List<Integer> orderList = new LinkedList<Integer>();
    for (int i=0;i<numIter;++i){
      InetSocketAddress addr = alg.route(null, 1, NodeList);
      orderList.add(addr.getPort());
    }
    TestCase.assertEquals(expectedList, orderList);
  }
}
