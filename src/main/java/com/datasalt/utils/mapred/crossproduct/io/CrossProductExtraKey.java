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

package com.datasalt.utils.mapred.crossproduct.io;


import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.thrift.TException;

import com.datasalt.utils.io.IdDatumBase;

public class CrossProductExtraKey extends IdDatumBase
{
	
  public CrossProductExtraKey() {
    super();
  }
  
  public CrossProductExtraKey(int groupId, byte[] group) throws TException {
  	super(groupId,group);
  }
    
  public int getGroupId() {
  	return getIdentifier();
  }
  
  public void setGroupId(int groupId) {
  	setIdentifier(groupId);
  }
  
  /**
   * Raw datum getter
   */
  public BytesWritable getGroup() {
  	return getItem1();
  }
  
  
  public void setGroup(byte[] datum,int offset,int length){
    setItem1(datum,offset,length);
  }
  
  public void setGroup(BytesWritable writable){
    setItem1(writable);
  }
  
  /**
   * Raw datum setting. 
   */
  public void setGroup(byte[] datum){
    setItem1(datum);
  }
  
  static {                                        // register this comparator
    WritableComparator.define(CrossProductExtraKey.class, new Comparator());
  }

}
