package zu.finagle.http;

import java.net.InetSocketAddress;

import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;

import zu.finagle.ZuFinagleService;

import com.twitter.finagle.Service;

public class ZuFinagleHttpService extends ZuFinagleService<HttpRequest, HttpResponse>{

  public ZuFinagleHttpService(Service<HttpRequest, HttpResponse> finagleSvc,
      InetSocketAddress addr) {
    super(finagleSvc, addr);
  }
  
}
