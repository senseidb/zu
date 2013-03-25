package com.senseidb.zu.finagle.serialize;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface ZuSerializer<Req, Res> {
  //String getName();
	Req deserializeRequest(ByteBuffer from) throws IOException;
	ByteBuffer serializeRequest(Req to) throws IOException;
  Res deserializeResponse(ByteBuffer from) throws IOException;
  ByteBuffer serializeResponse(Res to) throws IOException;
}
