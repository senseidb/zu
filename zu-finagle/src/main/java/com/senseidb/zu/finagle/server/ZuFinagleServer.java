package com.senseidb.zu.finagle.server;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.protocol.TBinaryProtocol;

import com.senseidb.zu.finagle.rpc.ZuThriftService;
import com.senseidb.zu.finagle.rpc.ZuTransport;
import com.senseidb.zu.finagle.serialize.ZuSerializer;
import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.thrift.ThriftServerFramedCodec;
import com.twitter.util.Future;

public class ZuFinagleServer implements ZuThriftService.ServiceIface{

  public interface RequestHandler<Req, Res> {
    String getName();
    Res handleRequest(Req req);
    ZuSerializer<Req, Res> getSerializer(); 
  }

  private final int port;
  private final String name;
  private final Map<String,RequestHandler<?,?>> reqHandlerMap;
  
  public ZuFinagleServer(String name, int port) {
    this.port = port;
    this.name = name;
    this.reqHandlerMap = new HashMap<String, RequestHandler<?,?>>();
  }
  
  public <Req, Res> void registerHandler(RequestHandler<Req, Res> reqHandler) {
    reqHandlerMap.put(reqHandler.getName(), reqHandler);
  }
  
  public void start() {
    ServerBuilder.safeBuild(new ZuThriftService.Service(this, new TBinaryProtocol.Factory()),
        ServerBuilder.get()
        .codec(ThriftServerFramedCodec.get())
        .name(name)
        .bindTo(new InetSocketAddress(port)));
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Future<ZuTransport> send(ZuTransport req) {
    String name = req.getName();
    RequestHandler handler = reqHandlerMap.get(name);
    if (handler == null) {
      return Future.exception(new IllegalArgumentException("handler "+name+" is not registered"));
    }
    try {
      ZuSerializer serializer = handler.getSerializer();
      Object reqObj = serializer.deserializeRequest(req.data);
      Object res = handler.handleRequest(reqObj);
      ByteBuffer bytes = serializer.serializeResponse(res);
    
      ZuTransport resp = new ZuTransport();
      resp.data = bytes;
      return Future.value(resp);
    }
    catch(Throwable th) {
      return Future.exception(th);
    }
  }
  
  public static void main(String[] args) throws Exception{
    ZuFinagleServer server = new ZuFinagleServer("", 8123);
    server.start();
  }
}
