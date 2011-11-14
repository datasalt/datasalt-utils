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

import static org.junit.Assert.*;

import java.io.IOException;

import com.datasalt.utils.thrift.test.A;
import com.datasalt.utils.thrift.test.B;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;

import com.datasalt.utils.commons.HadoopUtils;
import com.datasalt.utils.commons.test.BaseTest;
import com.datasalt.utils.mapred.joiner.MultiJoiner;
import com.datasalt.utils.mapred.joiner.TestMultiJoiner.AMapper;
import com.datasalt.utils.mapred.joiner.TestMultiJoiner.BMapper;
import com.datasalt.utils.mapred.joiner.TestMultiJoiner.TestReducer;

public class TestMultiJoinerGlob extends BaseTest {
	
	public static final String OUTPUT_FOR_TEST = "test-" +TestMultiJoinerGlob.class.getName();

	@Test
	public void test() throws IOException, InterruptedException, ClassNotFoundException {

		Configuration conf = getConf();
		MultiJoiner multiJoiner = new MultiJoiner("MultiJoiner Test",conf);
		multiJoiner.setReducer(TestReducer.class);
		multiJoiner.setOutputKeyClass(Text.class);
		multiJoiner.setOutputValueClass(Text.class);
		multiJoiner.setOutputFormat(TextOutputFormat.class);
		multiJoiner.setOutputPath(new Path(OUTPUT_FOR_TEST));
		
		Job job = multiJoiner
			.addChanneledInput(0, new Path("src/test/resources/glob-folder/*"), A.class, TextInputFormat.class, AMapper.class)
			.addChanneledInput(1, new Path("src/test/resources/multijoiner.test.b.txt"), B.class, TextInputFormat.class, BMapper.class)
			.getJob();
		job.waitForCompletion(true);
		assertTrue(job.isSuccessful());
		
		HadoopUtils.deleteIfExists(FileSystem.get(conf), new Path(OUTPUT_FOR_TEST));
	}
}
