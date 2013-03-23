package com.senseidb.zu.finagle.serialize;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface ZuSerializer<T> {
  String getName();
	T deserialize(ByteBuffer from) throws IOException;
	ByteBuffer serialize(T to) throws IOException;
}
