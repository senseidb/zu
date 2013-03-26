package zu.finagle.client;

import java.util.Map;

import com.twitter.util.Future;

public abstract class ZuScatterGatherer<Req, Res> {

  public Req rewrite(Req req, int shard){
    return req;
  }
  
  abstract public Future<Res> merge(Map<Integer, Res> results);
}
