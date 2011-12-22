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

package com.datasalt.utils.mapred.counter.io;



import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;

import com.datasalt.utils.io.IdDatumPairBase;
import com.datasalt.utils.mapred.counter.MapRedCounter;

/**
 * Class used as key in {@link MapRedCounter}
 * 
 * @author epalace
 */
public class CounterKey extends IdDatumPairBase{

	public CounterKey(){
		super();
	}
	
	 public CounterKey(int groupId, byte[] group, byte[] item) {
	  	super(groupId,group,item);
	  }
	  
	 
	  
	  public void set(int groupId, byte[] group, byte[] item) {
	  	super.set(groupId, group, item);
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
	  
	  /**
	   * Raw datum getter
	   */
	  public BytesWritable getItem() {
	  	return getItem2();
	  }
	  
	  /**
	   * Raw datum setting. 
	   */
	  
	  public void setGroup(BytesWritable b){
	    setItem1(b);
	  }
	  
	  public void setItem(BytesWritable b){
	    setItem2(b);
	  }
	  
	  public void setGroup(byte[] datum)  {
	    setItem1(datum);
	  }
	  
	  public void setGroup(byte[] datum,int offset,int length)  {
	    setItem1(datum,offset,length);
	  }

	  public void setItem(byte[] datum)  {
	    setItem2(datum);
	  }
	  
	  
	  /**
	   * Raw datum setting. 
	   */
	  public void setItem(byte[] datum,int offset,int length) {
	    setItem2(datum,offset,length);
	  }
	  
	  
	  /** A Comparator optimized for PairDatumRawComparable that only compares
	   * by the group Id and the group. */ 
	  public static class IdGroupComparator extends IdDatumPairBase.IdItem1Comparator {}
	  
	  /**
	   * Partitioner class that decides the partition only using the field
	   * typeIdentifier and item1. Needed to do properly the secondary sorting.
	   *  
	   * @author eric
	   */
	  public static class IdGroupPartitioner extends IdDatumPairBase.IdItem1Partitioner{}
	  
	  
	  
	  static {                                        // register this comparator
	    WritableComparator.define(CounterKey.class, new Comparator());
	  } 
	
	
}
