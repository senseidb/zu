package zu.finagle.test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import zu.finagle.serialize.JOSSSerializer;
import zu.finagle.serialize.ZuSerializer;
import zu.finagle.server.ZuTransportService;

import com.twitter.util.Future;

public class ReqServiceImpl implements ZuTransportService.RequestHandler<Req2, Resp2>, ReqService.ServiceIface{
  static final String SVC = "cluster";

  @SuppressWarnings("rawtypes")
  // using the default java serializer
  static final ZuSerializer serializer = new JOSSSerializer();
  
  private final Set<Integer> shards;
  public ReqServiceImpl(Set<Integer> shards){
    this.shards = shards;
  }
  
  public Set<Integer> getShards() {
    return shards;
  }
  
  @Override
  public String getName() {
    return SVC;
  }

  @Override
  public Resp2 handleRequest(Req2 req) {
    Future<Resp2> future = handle(req);
    try {
      return future.toJavaFuture().get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public ZuSerializer<Req2, Resp2> getSerializer() {
    return serializer;
  }

  @Override
  public Future<Resp2> handle(Req2 req) {
    Resp2 resp = new Resp2();
    resp.setVals(new HashSet<Integer>());
    if (shards.contains(req.getNum())){
      resp.vals.add(req.getNum());  
    }
    return Future.value(resp);
  }
}