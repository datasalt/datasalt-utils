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

package com.datasalt.utils.mapred.joiner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * This is the class that will be serialized as value when using the {@link MultiJoiner}.
 * The MultiJoiner API will serialize any object here by using the Hadoop Serialization API.
 * 
 * @author pere
 *
 * @param <T>
 */
public class MultiJoinDatum<T> implements Writable {

  private BytesWritable datum=new BytesWritable();
  private int channelId;
  
  // Default constructor needed.
  public MultiJoinDatum() {
    
  }

  public BytesWritable getDatum() {
  	return datum;
  }

	public void setDatum(byte[] datum) {
  	this.datum.set(datum,0,datum.length);
  }
	
	public void setDatum(byte[] datum,int offset,int length) {
    this.datum.set(datum,offset,length);
  }
	
	public void setDatum(BytesWritable b) {
    this.datum.set(b);
  }

	public int getChannelId(){
    return channelId;
  }
  
  public void setChannelId(int channelId){
    this.channelId = channelId;
  }  
  
  @Override
  public void readFields(DataInput in) throws IOException {
    channelId = WritableUtils.readVInt(in);
    datum.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out,channelId);
    datum.write(out);
  }
}