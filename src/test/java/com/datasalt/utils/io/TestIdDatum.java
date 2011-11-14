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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.datasalt.utils.thrift.test.A;

import org.apache.thrift.TException;
import org.junit.Test;

import com.datasalt.utils.commons.test.BaseTest;
import com.datasalt.utils.io.IdDatumBase;
import com.datasalt.utils.io.Serialization;

import static org.junit.Assert.*;

public class TestIdDatum extends BaseTest {

	@Test
	public void testSerialization() throws TException, IOException {
		Serialization ser = getSer();
		A a1 = new A();
		a1.setId("1");
		a1.setUrl("1");		

		
		IdDatumBase datum1 = new IdDatumBase(1, ser.ser(a1));

		assertEquals(1, datum1.getIdentifier());
		
		A a = new A();
		ser.deser(a, datum1.getItem1());
		assertEquals(a.getId(),  "1");
		assertEquals(a.getUrl(), "1");
		
		ByteArrayOutputStream oS = new ByteArrayOutputStream();
		datum1.write(new DataOutputStream(oS));
		byte[] bytes = oS.toByteArray();
		IdDatumBase datum2 = new IdDatumBase();
		ByteArrayInputStream iS = new ByteArrayInputStream(bytes);
		datum2.readFields(new DataInputStream(iS));
		
		assertEquals(1, datum2.getIdentifier());
		
		a = new A();
		ser.deser(a, datum2.getItem1());
		assertEquals(a.getId(),  "1");
		assertEquals(a.getUrl(), "1");
				
		assertEquals(datum1.hashCode(), datum2.hashCode());
		assertEquals(datum1, datum2);
		assertEquals(0, datum1.compareTo(datum2));
	}
	
	
	@Test
	public void testItemsDifferenciation() throws TException, IOException{
		Serialization ser = getSer();
		A a1 = new A();
		a1.setId("1");
		a1.setUrl("1");		

		A a11 = new A();
		a11.setId("11");
		a11.setUrl("11");

		// Different 
		
		IdDatumBase datum1 = new IdDatumBase((short) 1, ser.ser(a1));
		IdDatumBase datum2 = new IdDatumBase((short) 1, ser.ser(a11));
		
		int normalCmp = datum1.compareTo(datum2);
		assertTrue(0 != normalCmp);

		ByteArrayOutputStream oS = new ByteArrayOutputStream();
		datum1.write(new DataOutputStream(oS));
		byte[] d1 = oS.toByteArray();
		
		oS = new ByteArrayOutputStream();
		datum2.write(new DataOutputStream(oS));
		byte[] d2 = oS.toByteArray();
		
		int rawCmp = new IdDatumBase.Comparator().compare(d1, 0, d1.length, d2, 0, d2.length);
		assertTrue(0 != rawCmp);
		assertTrue(rawCmp < 0 && normalCmp < 0);
		
		// Equals
		
		datum1 = new IdDatumBase((short) 1, ser.ser(a11));
		datum2 = new IdDatumBase((short) 1, ser.ser(a11));
		
		normalCmp = datum1.compareTo(datum2);
		assertTrue(0 == normalCmp);

		oS = new ByteArrayOutputStream();
		datum1.write(new DataOutputStream(oS));
		d1 = oS.toByteArray();
		
		oS = new ByteArrayOutputStream();
		datum2.write(new DataOutputStream(oS));
		d2 = oS.toByteArray();
		
		rawCmp = new IdDatumBase.Comparator().compare(d1, 0, d1.length, d2, 0, d2.length);
		assertTrue(0 == rawCmp);

	}
	
	
}