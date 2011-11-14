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

package com.datasalt.utils.count;

import static org.junit.Assert.*;





import org.junit.Test;

import com.datasalt.utils.commons.count.Counter;
import com.datasalt.utils.commons.count.Counter.Count;
import com.datasalt.utils.commons.count.Counter.CounterException;

public class TestCounter {
	
	@Test
	public void testDistinct() throws CounterException {

		Counter c = Counter.createWithDistinctElements();
		c.in("Animals").in("Dog").in("Mastin").count("toby");
		c.in("Animals").in("Dog").in("Mastin").count("toby");
		c.in("Animals").in("Dog").in("Mastin").count("toby");
		c.in("Animals").in("Dog").in("Mastin").count("toby");
		
		Count co = c.getCounts();
		assertEquals(1, co.getDistinctList().size());
		assertEquals(1, co.get("Animals").getDistinctList().size());
		assertEquals(1, co.get("Animals").get("Dog").getDistinctList().size());
		assertEquals(1, co.get("Animals").get("Dog").get("Mastin").getDistinctList().size());
	}
	
	@Test
	public void test() throws Counter.CounterException {
		Counter c = Counter.createWithDistinctElements();
		c.in("Animals").in("Dog").in("Mastin").count("toby");
		c.in("Animals").in("Dog").in("BullDog").count("toby");
		c.in("Animals").in("Dog").in("BullDog").count("toby");
		c.in("Animals").in("Dog").in("BullDog").count("brian");
		
		Count co = c.getCounts();
		assertEquals(4, co.getCount());
		assertEquals(2, co.getDistinctList().size());
		
		assertEquals(4, co.get("Animals").get("Dog").getCount());
		assertEquals(2, co.get("Animals").get("Dog").getDistinctList().size());

		assertEquals(1, co.get("Animals").get("Dog").get("Mastin").getCount());
		assertEquals(1, co.get("Animals").get("Dog").get("Mastin").getDistinctList().size());
		
		assertEquals(3, co.get("Animals").get("Dog").get("BullDog").getCount());
		assertEquals(2, co.get("Animals").get("Dog").get("BullDog").getDistinctList().size());
	}
	
}
