package zu.finagle.serialize;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.zookeeper.server.ByteBufferInputStream;

public class JOSSSerializer<Req extends Serializable, Res extends Serializable> implements ZuSerializer<Req, Res> {

  @Override
  @SuppressWarnings({"unchecked"})
  public Req deserializeRequest(ByteBuffer from) throws IOException {
    ObjectInputStream oin = new ObjectInputStream(new ByteBufferInputStream(from));
    try {
      return (Req)oin.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e.getMessage(), e);
    }
    finally{
      oin.close();
    }
  }

  @Override
  public ByteBuffer serializeRequest(Req to) throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    ObjectOutputStream oout = new ObjectOutputStream(bout);
   
    oout.writeObject(to);
    oout.flush();
    oout.close();
   
    return ByteBuffer.wrap(bout.toByteArray());
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public Res deserializeResponse(ByteBuffer from) throws IOException {
    ObjectInputStream oin = new ObjectInputStream(new ByteBufferInputStream(from));
    try {
      return (Res)oin.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e.getMessage(), e);
    }
    finally{
      oin.close();
    }
  }

  @Override
  public ByteBuffer serializeResponse(Res to) throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    ObjectOutputStream oout = new ObjectOutputStream(bout);
    
    oout.writeObject(to);
    oout.flush();
    oout.close();
    
    return ByteBuffer.wrap(bout.toByteArray());
  }

}
