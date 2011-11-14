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

import com.datasalt.utils.io.DatumPairBase;
import com.datasalt.utils.mapred.crossproduct.CrossProductMapRed;

/**
 * Used as output of {@link CrossProductMapRed}
 * @author epalace
 *
 */
public class CrossProductPair extends DatumPairBase{
//Default constructor needed.
  public CrossProductPair() {
    super();
  }
  
  public CrossProductPair(byte [] left, byte[] right) throws TException {
    super(left,right);
  }
    
  /**
   * Raw datum getter
   */
  public BytesWritable getLeft() {
  	return getItem1();
  }
  
  /**
   * Raw datum getter
   */
  public BytesWritable getRight() {
  	return getItem2();
  }
    
  /**
   * Raw datum setting. 
   */
  public void setLeft(byte[] item) {
    setItem1(item);
  }
  
  public void setLeft(byte[] datum,int offset,int length) {
    setItem1(datum,offset,length);
  }
  
  public void setLeft(BytesWritable datum) {
    setItem1(datum);
  }
  
  
  /**
   * Raw datum setting. 
   */
  public void setRight(byte[] datum) {
    setItem2(datum);
  }
  
  public void setRight(byte[] datum,int offset,int length) {
    setItem2(datum,offset,length);
  }
  
  public void setRight(BytesWritable datum) {
    setItem2(datum);
  }
	
  static {                                        // register this comparator
    WritableComparator.define(CrossProductPair.class, new ComparatorWithNoOrder());
  }
  
}
