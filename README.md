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

### Code snippet:

    // Zookeepr location
    String zkHost = ...
    int zkPort = ...

    // implementation provides a mapping between a url and its partitions
    PartitionInfoReader partitionInfoReader = ...

    ZuCluster cluster = new ZuCluster(new InetSocketAddress(zkHost,zkPort), partitionInfoReader, "mycluster");

    cluster.addClusterEventListener(new ZuClusterEventListener() {

	      @Override
	      public void clusterChanged(Map<Integer, ArrayList<InetSocketAddress>> clusterView) {
		    System.out.println("cluster changed");
	      }

	      @Override
	      public void nodesRemovedFromCluster(List<InetSocketAddress> nodes) {
		    System.out.println("nodes removed from cluster");
	      }
	 });

    // join a cluster
    EndPointStatus endpoint = cluster.join(new InetSocketAddress(host,port));

    // leaving a cluster
    cluster.leave(endpoint);
