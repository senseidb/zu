package zu.core.cluster;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common.zookeeper.ZooKeeperClient.Credentials;

public class ZookeeperClientBuilder {
  private static final Logger logger = LoggerFactory.getLogger(ZookeeperClientBuilder.class);
  
  private List<InetSocketAddress> addrList = Lists.newLinkedList();
  private Credentials credentials = Credentials.NONE;
  private Amount<Integer, Time> sessionTimeout = Amount
      .of(ZuCluster.DEFAULT_TIMEOUT, Time.SECONDS);

  public ZookeeperClientBuilder setCredentials(Credentials credentials) {
    this.credentials = credentials;
    return this;
  }

  public ZookeeperClientBuilder setSessionTimeout(int seconds) {
    sessionTimeout = Amount.of(seconds, Time.SECONDS);
    return this;
  }

  public ZookeeperClientBuilder addZookeeperUrl(String zkUrl) {
    addrList.addAll(parseZookeeperUrl(zkUrl));
    return this;
  }

  public ZookeeperClientBuilder setZookeeperUrl(String zkUrl) {
    addrList = parseZookeeperUrl(zkUrl);
    return this;
  }

  public ZooKeeperClient build() {
    return new ZooKeeperClient(sessionTimeout, credentials, addrList);
  }

  public static Set<String> getAvailableClusters(ZooKeeperClient zkClient, String prefix, boolean watch)
      throws Exception {
    try {
      ZooKeeper zk = zkClient.get();    
      return ImmutableSet.copyOf(zk.getChildren(prefix, watch));
    } catch(Exception e) {
      logger.error(e == null ? "cannot get cluster information" : e.getMessage());
      return Collections.emptySet();
    }
  }

  public static Map<Integer, List<InetSocketAddress>> getClusterView(
      String prefix, String clusterId, ZooKeeperClient zkClient) throws Exception {
    AtomicReference<Map<Integer, List<InetSocketAddress>>> mapRef = new AtomicReference<>();
    ZuCluster cluster = null;

    try {
      cluster = new ZuCluster(zkClient, prefix, clusterId, false);
      cluster.addClusterEventListener(new ZuClusterEventListener() {

        @Override
        public void nodesRemoved(Set<InetSocketAddress> removedNodes) {
        }

        @Override
        public void clusterChanged(
            Map<Integer, List<InetSocketAddress>> clusterView) {
          mapRef.set(clusterView);
        }
      });
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
    return mapRef.get();
  }  
  
  public static List<InetSocketAddress> parseZookeeperUrl(String url) {
    List<InetSocketAddress> addrList = Lists.newLinkedList();
    if (url != null && url.length() > 0) {
      String[] hostPortPairStrings = url.split(",");    
      if (hostPortPairStrings != null && hostPortPairStrings.length > 0) {
        for (String hostPortPairString : hostPortPairStrings) {
          InetSocketAddress addr = parseHostPort(hostPortPairString);
          if (addr != null) {
            addrList.add(addr);
          }
        }
      }
    }
    
    return addrList;
  }
  
  public static InetSocketAddress parseHostPort(String hostPort) {    
    try {
      if (hostPort != null) {
        String[] hostPortParts = hostPort.split(":");
        String host = hostPortParts[0];
        int port = Integer.parseInt(hostPortParts[1]);
        return new InetSocketAddress(host, port);
      } else {
        return null;
      }
    } catch(Exception e) {
      logger.error("cannot parse host port string: " + hostPort);
      return null;
    }
  }
}
