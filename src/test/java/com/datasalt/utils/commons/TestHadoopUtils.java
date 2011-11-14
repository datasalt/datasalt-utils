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

package com.datasalt.utils.commons;

import static org.junit.Assert.assertEquals;

import java.io.IOException;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.datasalt.utils.commons.HadoopUtils;
import com.datasalt.utils.commons.test.BaseTest;

/**
 * Test HadoopUtils class
 * 
 * @author ivan
 */
public class TestHadoopUtils extends BaseTest {

	@Test
	public void testStringToFile() throws IOException {
		FileSystem fs = FileSystem.getLocal(getConf());
		Path path = new Path(TestHadoopUtils.class.getCanonicalName());
		
		try {
			
			String text = "String\nDe Prueba";
			
			for (int i=0;i<10;i++) {
				text += text;
			}
			
			HadoopUtils.stringToFile(fs, path, text);
			String read = HadoopUtils.fileToString(fs, path);
			
			assertEquals(text, read);
		} finally {
			fs.delete(path, true);
		}
	}


}
