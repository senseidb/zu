package zu.finagle.serialize;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.thrift.TBase;
import org.apache.zookeeper.server.ByteBufferInputStream;

import com.twitter.common.io.ThriftCodec;

public class ThriftSerializer<Req extends TBase<?,?>, Res extends TBase<?,?>> implements ZuSerializer<Req, Res> {

  private final ThriftCodec<Req> reqCodec;
  private final ThriftCodec<Res> resCodec;
  
  public ThriftSerializer(Class<Req> reqClass, Class<Res> resClass) {
    reqCodec = ThriftCodec.create(reqClass, ThriftCodec.BINARY_PROTOCOL);
    resCodec = ThriftCodec.create(resClass, ThriftCodec.BINARY_PROTOCOL);
  }
  
  @Override
  public Req deserializeRequest(ByteBuffer from) throws IOException {
    ByteBufferInputStream bin = new ByteBufferInputStream(from);
    return reqCodec.deserialize(bin);
  }

  @Override
  public ByteBuffer serializeRequest(Req to) throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    reqCodec.serialize(to, bout);
    bout.flush();
    return ByteBuffer.wrap(bout.toByteArray());
  }

  @Override
  public Res deserializeResponse(ByteBuffer from) throws IOException {
    ByteBufferInputStream bin = new ByteBufferInputStream(from);
    return resCodec.deserialize(bin);
  }

  @Override
  public ByteBuffer serializeResponse(Res to) throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    resCodec.serialize(to, bout);
    bout.flush();
    return ByteBuffer.wrap(bout.toByteArray());
  }
}
