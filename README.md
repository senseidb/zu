# Zu - A simple cluster management system based on Twitter's ServerSets api

### What is it?

A simple cluster management system for partitioned data. E.g. a distributed search engine.

Zu is built on top of Twitter's Zookeeper based server management api called ServerSets.

### Where does the name mean?

Zu comes from the Chinese character: 组, which means a group or a set.

### What are some of Zu's features?

+ Simple api
+ Manages partitioned data
+ Integration with Twitter's Finagle

### Build it:

    mvn clean package

2 artifacts:

+ zu-core - core library
+ zu-finagle - Finagle support

### Maven:

##### Core:
    <groupid>com.senseidb.zu</groupid>
    <artifactId>zu-core</artifactId>
##### Finagle:
    <groupid>com.senseidb.zu</groupid>
    <artifactId>zu-finagle</artifactId>

### Code snippet:

    // Zookeepr location
    String zkHost = ...
    int zkPort = ...

    ZuCluster cluster = new ZuCluster(new InetSocketAddress(zkHost,zkPort), "mycluster");

    cluster.addClusterEventListener(new ZuClusterEventListener() {

	      @Override
	      public void clusterChanged(Map<Integer, ArrayList<InetSocketAddress>> clusterView) {
		    // a shard -> host mapping
		    System.out.println("cluster changed");
	      }

	      @Override
	      public void nodesRemovedFromCluster(List<InetSocketAddress> nodes) {
		    System.out.println("nodes removed from cluster");
	      }
	 });

    // join a cluster
    List<EndPointStatus> endpoints = cluster.join(new InetSocketAddress(host,port), Arrays.asList(1,2));

    // leaving a cluster
    cluster.leave(endpoints);

### More details:

Need more, check out the unit tests:

Simple cluster setup:
[Example 1.](https://github.com/javasoze/zu/blob/master/zu-core/src/test/java/zu/core/test/ZuTest.java)

A full finagle based http cluster of servers:
[Example 2.](https://github.com/javasoze/zu/blob/master/zu-finagle/src/test/java/zu/finagle/test/ZuFinagleTest.java)