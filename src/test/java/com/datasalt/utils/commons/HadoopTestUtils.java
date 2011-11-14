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

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datasalt.utils.commons.HadoopUtils;

public class HadoopTestUtils {

	/**
	 * Creates a file with just one line. Useful for test mapreduce jobs. It
	 * allows you create a mapper. The map function of this mapper will be called
	 * only once. This call can be used to emit whatever you need.  
	 * @throws IOException 
	 */
	public static void oneLineTextFile(FileSystem fs, Path path) throws IOException {
		HadoopUtils.stringToFile(fs, path, "Dummy Row");
	}
}
