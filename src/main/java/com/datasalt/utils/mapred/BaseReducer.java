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

package com.datasalt.utils.mapred;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.datasalt.utils.io.Serialization;

/**
 * 
 * A reducer that includes methods for serialize/deserialize
 * 
 * @author ivan
 */
public class BaseReducer<IKey, IValue, OKey, OValue> extends Reducer<IKey, IValue, OKey, OValue> {

	/*
	 * For JSON Ser/De
	 */
	//public final static TypeReference<HashMap<String, Object>> MAP = new TypeReference<HashMap<String, Object>>() {
	//};
	//protected ObjectMapper mapper = new ObjectMapper();

	protected Serialization ser;
	
	/**
	 * Configures the serialization
	 */
	@Override
  protected void setup(Context context) throws IOException, InterruptedException {
	  super.setup(context);
	  
	  ser = new Serialization(context.getConfiguration());
  }
	
	/**
	 * Method for serialize. See {@link Serialization#ser(Object)} 
	 */
	public byte[] ser(Object datum) throws IOException {
		return ser.ser(datum);
	}
	
	/**
	 * Metod for deserialize. See {@link Serialization#deser(Object, BytesWritable)}
	 */
	@SuppressWarnings("unchecked")
  public <T> T deser(Object obj, BytesWritable writable) throws IOException {
		return (T) ser.deser(obj, writable);
	}
}
