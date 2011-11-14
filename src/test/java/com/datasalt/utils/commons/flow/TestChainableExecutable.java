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

package com.datasalt.utils.commons.flow;

import static org.junit.Assert.*;

import org.junit.Test;

import com.datasalt.utils.commons.flow.ChainableExecutable;
import com.datasalt.utils.commons.flow.Executable;

public class TestChainableExecutable {

	@Test
	public void test() throws Exception {
		
		final StringBuffer sbOrder = new StringBuffer();
		final StringBuffer sbWrite = new StringBuffer();
		
		Executable<String> exA = new Executable<String>() {

			@Override
      public void execute(String configData) throws Exception {
				sbWrite.append(configData);
				sbOrder.append("A");
      }
			
		};
		
		Executable<String> exB = new Executable<String>() {

			@Override
      public void execute(String configData) throws Exception {
				sbWrite.append(configData);
				sbOrder.append("B");
      }
			
		};
		
		Executable<String> exC = new Executable<String>() {

			@Override
      public void execute(String configData) throws Exception {
				sbWrite.append(configData);
				sbOrder.append("C");
      }
			
		};


 		
		ChainableExecutable<String> c1 = new ChainableExecutable<String>("c1", "c1des", null, exA);
		ChainableExecutable<String> c2 = new ChainableExecutable<String>("c2", "c2des", c1, exB);
		ChainableExecutable<String> c3 = new ChainableExecutable<String>("c3", "c3des", c1, exC);
		c3.registerChild(exC);
		
		c1.execute("A");
		
		assertEquals("ABCC", sbOrder.toString());
		assertEquals("AAAA", sbWrite.toString());
		assertEquals(c1.getName(), "c1");
		assertEquals(c1.getDescription(), "c1des");
		
	}
}
