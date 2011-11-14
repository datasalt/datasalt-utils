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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.datasalt.utils.thrift.test.A;

import org.apache.thrift.TException;
import org.junit.Test;

import com.datasalt.utils.commons.ThriftUtils;
import com.datasalt.utils.mapred.joiner.MultiJoinDatum;

public class TestMultiJoinDatum {

	@Test
	public void test() throws TException, IOException {
		MultiJoinDatum<A> datum = new MultiJoinDatum<A>();
		A a = new A();
		a.setId("id1");
		a.setUrl("url");
		datum.setChannelId((byte)2);
		datum.setDatum(ThriftUtils.getSerializer().serialize(a));
		
		ByteArrayOutputStream oS = new ByteArrayOutputStream();
		datum.write(new DataOutputStream(oS));
		byte[] bytes = oS.toByteArray();
		datum = new MultiJoinDatum<A>();
		ByteArrayInputStream iS = new ByteArrayInputStream(bytes);
		datum.readFields(new DataInputStream(iS));
		
		a = new A();
		byte[] newArray = new byte[datum.getDatum().getLength()];
		System.arraycopy(datum.getDatum().getBytes(),0,newArray,0,datum.getDatum().getLength());
		ThriftUtils.getDeserializer().deserialize(a,newArray);
		assertEquals(a.getId(),  "id1");
		assertEquals(a.getUrl(), "url");
		assertEquals(datum.getChannelId(), 2);
	}
}
