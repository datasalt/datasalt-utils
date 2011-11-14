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

package com.datasalt.utils.commons.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

/**
 * This utility can be used to dump a tabulated key -> value text file as a sequencefile of [Text, Text]
 * <p>
 * Input will be fetched locally. Output will be written locally or in the remote DFS depending on your 
 * Hadoop configuration.
 * 
 * @author pere
 *
 */
public class DumpTextFileAsSequenceFile {

	public static void dump(String input, String output) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fS = FileSystem.get(conf);
				
		BufferedReader reader = new BufferedReader(new FileReader(new File(input)));
		String line = "";
		Text t1 = new Text();
		Text t2 = new Text();
		
		SequenceFile.Writer writer = new SequenceFile.Writer(fS, conf, new Path(output), Text.class, Text.class);
		
		while((line = reader.readLine()) != null) {
			String[] fields = line.split("\t");
			t1.set(fields[0]);
			t2.set(fields[1]);
			writer.append(t1, t2);
		}
		writer.close();
		reader.close();		
	}
	
	public final static void main(String[] args) throws IOException {
		if(args.length != 2) {
			throw new IllegalArgumentException("Number of args shoul be 2: [input] [output]" +
				"\n\nInput will be fetched locally. Output will be written locally or in the remote" +
				" DFS depending on your Hadoop configuration.");
		}
		dump(args[0], args[1]);
	}
}
