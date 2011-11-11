package com.datasalt.utils.commons.io;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.util.ReflectionUtils;

import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtobufIOUtil;
import com.dyuproject.protostuff.Schema;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class ProtoStuffDeserializer<T extends Schema> implements Deserializer<T> {
  
	InputStream in;
  private Class<T> tClass;
  
  public ProtoStuffDeserializer(Class<T> tClass) {
  	this.tClass = tClass;
  }
  
	static ThreadLocal<LinkedBuffer> threadLocalBuffer = new ThreadLocal<LinkedBuffer>() {

		LinkedBuffer buffer = LinkedBuffer.allocate(512);

		@Override
		public LinkedBuffer get() {
			return buffer;
		}
	};
	
	@Override
  public void open(InputStream in) throws IOException {
		this.in = in;
	}

  @Override
  public T deserialize(T t) throws IOException {
  	t = t == null ? newInstance() : t;
		LinkedBuffer buff = threadLocalBuffer.get();
		ProtobufIOUtil.mergeDelimitedFrom(in, t, t, buff);
		buff.clear();
		return t;
  }

  private T newInstance() {
    return (T) ReflectionUtils.newInstance(tClass, null);
  }
  
	@Override
  public void close() throws IOException {

	}
}
