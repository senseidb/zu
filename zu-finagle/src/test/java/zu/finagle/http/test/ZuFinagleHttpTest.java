package zu.finagle.http.test;

import java.net.InetSocketAddress;

import junit.framework.TestCase;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.Test;

import zu.finagle.ZuFinagleService;
import zu.finagle.http.ZuFinagleHttpServiceFactory;

import com.twitter.finagle.Service;
import com.twitter.finagle.builder.Server;
import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.http.Http;
import com.twitter.util.Future;

public class ZuFinagleHttpTest {

  @Test
  public void testBasic() throws Exception{
    
    final String val = "test";
    final int port = 4444;
    Service<HttpRequest, HttpResponse> svc = new Service<HttpRequest,HttpResponse>(){

      @Override
      public Future<HttpResponse> apply(HttpRequest req) {
        HttpResponse httpResponse =
            new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
          httpResponse.setContent(ChannelBuffers.wrappedBuffer(val.getBytes()));
          return Future.value(httpResponse);
      }
    };
    
    Server svr = ServerBuilder.safeBuild(svc,
        ServerBuilder.get()
                     .codec(Http.get())
                     .name("HTTPServer")
                     .bindTo(new InetSocketAddress("localhost", port)));
    
    System.out.println(svr.localAddress());
    
    // building client via zu
    
    ZuFinagleHttpServiceFactory clientFacotry = new ZuFinagleHttpServiceFactory(1, 1000);
    ZuFinagleService<HttpRequest,HttpResponse> zuClient = clientFacotry.getService(new InetSocketAddress(port));
    
    Service<HttpRequest,HttpResponse> finagleClient = zuClient.getFinagleSvc();
    
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    
    HttpResponse response = finagleClient.apply(request).get();
    
    String resStr = new String(response.getContent().array());

    
    TestCase.assertEquals(val, resStr);
    

    
  }
}
