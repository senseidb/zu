package zu.core.cluster.util;

import java.io.File;
import java.net.InetSocketAddress;

import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;

public class Util {

  public static void rmDir(File dir){
    if (dir.isDirectory()){
      File[] files = dir.listFiles();
      for (File f : files){
        rmDir(f);
      }
    }
    dir.delete();
  }
  
  public static NIOServerCnxn.Factory startZkServer(int port, File dir) throws Exception{
    int tickTime = 2000;
    int numConnections = 10;

    dir = dir.getAbsoluteFile();

    ZooKeeperServer server = new ZooKeeperServer(dir, dir, tickTime);
    NIOServerCnxn.Factory standaloneServerFactory = new NIOServerCnxn.Factory(new InetSocketAddress(port), numConnections);
    standaloneServerFactory.startup(server);
    return standaloneServerFactory;
  }
}
