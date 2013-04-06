package zu.core.cluster.routing;

import java.util.Map;

public abstract class ZuScatterGatherer<Req, Res> {

  public Req rewrite(Req req, int shard){
    return req;
  }
  
  abstract public Res merge(Map<Integer, Res> results);
}
