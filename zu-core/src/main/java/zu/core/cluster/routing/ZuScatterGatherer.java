package zu.core.cluster.routing;

import java.util.Map;

/**
 * An interface for the scatter-gather pattern
 * @param <Req> request
 * @param <Res> response
 */
public abstract class ZuScatterGatherer<Req, Res> {

  /**
   * Rewrite a request given a shard id. The returned request may need to be cloned or deep-copied.
   * @param req request object
   * @param shard shard id
   * @return a modified request object
   */
  public Req rewrite(Req req, int shard){
    return req;
  }
  
  /**
   * merges a set of sub-results
   * @param results sub-result map for results returned from sub-shards
   * @return merged result
   */
  abstract public Res merge(Map<Integer, Res> results);
}
