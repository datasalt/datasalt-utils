/**
 * Copyright [2011] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datasalt.utils.commons;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.Writable;

/**
 * Class with utilities for {@link Writable} classes
 * 
 * @author ivan
 */
public class WritableUtils {

	/**
   * Serialize writable
   */
   public static byte[] serialize(Writable datum) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
  
    datum.write(dos);
    return bos.toByteArray();
  }

	/**
   * Deserialize Writables
   */
  public static Writable deserialize(Writable datum, byte [] bytes) throws IOException  {
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    DataInputStream dis = new DataInputStream(bis);
    
    datum.readFields(dis);
    
    dis.close();
    return datum;  	
  }

  static ThreadLocal<ByteBuffer> shortBuffer = new ThreadLocal<ByteBuffer>() {

		@Override
    protected ByteBuffer initialValue() {
	    return ByteBuffer.allocate(2); 
	  }
  			
  };
  
  /**
   * Reads a short from the bytes array start position.
   */
  public static short readShort(byte [] bytes, int start) {  	
    ByteBuffer bb = shortBuffer.get();
    bb.clear();
    bb.put(bytes[start + 0 ]);
    bb.put(bytes[start + 1]);
    return bb.getShort(0);
  }
}