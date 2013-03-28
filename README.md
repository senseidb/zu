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
+ Multi-tenancy for Finagle
+ Custom serialization
+ Scatter-gather support
+ Customizable requesting routing algorithm

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

### Zu Core:

+ Cluster management
+ Routing algorithm API

#### Code snippet:

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
    List<EndPointStatus> endpoints = cluster.join(new InetSocketAddress(host,port), new HashSet<Integer>)(Arrays.asList(1,2)));

    // leaving a cluster
    cluster.leave(endpoints);

For more details, checkout the unit tests [here](https://github.com/javasoze/zu/blob/master/zu-core/src/test/java/zu/core/test).

### Zu Finagle

Zu finagle is an extension to [Twitter's](http://twitter.github.com/) [Finagle](http://twitter.github.com/finagle/) with the following enhancements:

(To learn more about Finagle, checkout [this great example](https://github.com/jghoman/finagle-java-example))

+ Multi-tenancy - add handlers for different APIs without rebuilding the service IDL
+ Custom serializer - define different rpc serializer for each handler
+ Scatter-gather support

#### Example 1:

This example defines an API that returns the length of a string and is then deployed as a server.

See code listing [here](https://github.com/javasoze/zu/blob/master/zu-finagle/src/test/java/zu/finagle/test/ZuFinagleTest.java).

#### Example 2:

This example defines a set of 3 finagle servers, each handles different shards. They all join a cluster with scatter-gather logic defined.

We construct a broker based on the provided scatter-gather logic with random request routing logic for each shard.

See code listing [here](https://github.com/javasoze/zu/blob/master/zu-finagle/src/test/java/zu/finagle/test/StandardFinagleClusterTest.java).

#### Example 3:

We have the same setup as example 2, but instead of starting the finagle servers as finagle services, we wrap them up as ZuFinagleServers.

We then create the broker both as a finagle service as well a ZuFinagleServer in different tests.

See code listing [here](https://github.com/javasoze/zu/blob/master/zu-finagle/src/test/java/zu/finagle/test/ZuFinagleClusterTest.java).

#### Example 4:

Same as example 3, but instead of starting the ZuFinagleServers the wraps finagle services, we register our API as a ZuTransportService. This allows us to add different handlers while abstracting finagle's inner details as well as without having to re-compile the underlying thrift IDLs.

See code listing [here](https://github.com/javasoze/zu/blob/master/zu-finagle/src/test/java/zu/finagle/test/ZuFinagleClusterTest.java).