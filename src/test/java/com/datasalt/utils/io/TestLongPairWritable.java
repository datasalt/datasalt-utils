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

package com.datasalt.utils.io;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;


import org.junit.Test;

import com.datasalt.utils.commons.WritableUtils;
import com.datasalt.utils.io.LongPairWritable;

public class TestLongPairWritable {

  @Test  
  public void testSerialization() throws IOException {
    LongPairWritable p = new LongPairWritable(1, 2);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);

    p.write(dos);
    dos.close();
    bos.close();

    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    DataInputStream dis = new DataInputStream(bis);
    LongPairWritable s = new LongPairWritable();
    s.readFields(dis);
    assertEquals(1, p.getValue1());
    assertEquals(2, p.getValue2());
    assertEquals(p.getValue1(), s.getValue1()); 
    assertEquals(p.getValue2(), s.getValue2());
    
    assertEquals(p.hashCode(), s.hashCode());
    assertEquals(0, p.compareTo(s));
    assertTrue(p.equals(s));
  }
  
  static int rawCmp(byte[] b1, byte[] b2) {
  	return new LongPairWritable.Comparator().compare(b1, 0, b1.length, b2, 0, b2.length);
  }
  
  static int decreRawCmp(byte[] b1, byte[] b2) {
  	return new LongPairWritable.DecreasingComparator().compare(b1, 0, b1.length, b2, 0, b2.length);
  }

  
  @Test
  public void testComparator() throws IOException {
  	LongPairWritable p12 = new LongPairWritable(1, 2);
  	LongPairWritable p13 = new LongPairWritable(1, 3);
  	LongPairWritable p21 = new LongPairWritable(2, 1);
  	
  	byte[] b12 = WritableUtils.serialize(p12);
  	byte[] b13 = WritableUtils.serialize(p13);
  	byte[] b21 = WritableUtils.serialize(p21);
  	
  	int normCmp = p12.compareTo(p12);
  	int rawCmp = rawCmp(b12, b12);
  	
  	assertEquals(normCmp, rawCmp);
  	assertEquals(0, normCmp);
  	
  	normCmp = p12.compareTo(p13);
  	rawCmp = rawCmp(b12, b13);
  	
  	assertTrue(normCmp < 0 && rawCmp <0);
  	
  	normCmp = p12.compareTo(p21);
  	rawCmp = rawCmp(b12, b21);
  	
  	assertTrue(normCmp < 0 && rawCmp <0);

  	normCmp = p21.compareTo(p13);
  	rawCmp = rawCmp(b21, b13);
  	int decreRawCmp = decreRawCmp(b21, b13);
  	
  	assertTrue(normCmp > 0 && rawCmp > 0);
  	assertTrue(decreRawCmp < 0);
  }
  
}
