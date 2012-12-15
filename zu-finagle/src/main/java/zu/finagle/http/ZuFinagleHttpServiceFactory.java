package zu.finagle.http;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;

import zu.finagle.ZuFinagleService;
import zu.finagle.ZuFinagleServiceFactory;

import com.twitter.finagle.Service;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.http.Http;
import com.twitter.util.Duration;

public class ZuFinagleHttpServiceFactory extends
    ZuFinagleServiceFactory<HttpRequest, HttpResponse> {

  public ZuFinagleHttpServiceFactory(int numThreads, long timeout) {
    super(numThreads, timeout);
  }
  

  protected final Service<HttpRequest,HttpResponse> buildFinagleService(InetSocketAddress addr){
    return 
        ClientBuilder.safeBuild(ClientBuilder.get()
            .hosts(addr)
        .codec(Http.get())
        .requestTimeout(Duration.apply(timeout, TimeUnit.MILLISECONDS))
        .hostConnectionLimit(numThreads));
  }
  

  @Override
  public ZuFinagleService<HttpRequest, HttpResponse> getService(
      Service<HttpRequest, HttpResponse> client) {
    return null;
  }

}
