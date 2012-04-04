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

import java.net.URL;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.thrift.TBase;

import com.datasalt.utils.commons.BaseJob;
import com.datasalt.utils.commons.ThriftUtils;

/**
 * Utility for seing Sequence Files content as text. Thrift objects are dump as human readable JSON. The rest of objects
 * are dump by calling the toString()
 * 
 * @author ivan
 */
@SuppressWarnings({ "rawtypes" })
public class DumpSequenceFileAsText extends BaseJob {

	private final static String HELP = "Arguments: [options] [input] \n\n"
	    + "Utility for seen Sequence Files content as text. Thrift objects are dump"
	    + "as humal readable JSON. The rest of objects are dump by calling the toString() \n\n" + "Options:\n\n"
	    + "\t\t-l\t\tUse local filesystem\n" + "\t\t-p pos\t\tStart at the given file position\n"
	    + "\t\t-n rows\t\tReturn only n rows";

	public DumpSequenceFileAsText() {
	}

	@Override
	public void execute(String[] args, Configuration conf) throws Exception {
		boolean localFilesystem = false;
		long pos = -1;
		long rows = -1;

		// Command line parsing
		ArrayList<String> parameters = new ArrayList<String>();
		for(int i = 0; i < args.length; i++) {
			if("-l".equals(args[i])) {
				localFilesystem = true;
			} else if("-p".equals(args[i])) {
				pos = new Long(args[++i]);
			} else if("-n".equals(args[i])) {
				rows = new Long(args[++i]);
			} else {
				parameters.add(args[i]);
			}
		}

		if(parameters.size() != 1) {
			System.out.println(HELP);
			throw new IllegalArgumentException("Invalid number of arguments");
		}

		FileSystem fs;
		if(localFilesystem) {
			fs = FileSystem.getLocal(conf);
		} else {
			fs = FileSystem.get(conf);
		}

		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(parameters.get(0)), conf);

		// Syncing the file at a position
		if(pos != -1) {
			reader.sync(pos);
		}

		// Creating objects for reading
		Object key = reader.getKeyClass().newInstance();
		Object value = null;
		try {
			value = reader.getValueClass().newInstance();
		} catch(Exception e) {
			System.err.println(" Warning : Couldn't get a reader class instantated : " + e.getMessage()
			    + " --- NullWritable objects are known to cause this problem.");
		}

		int count = 0;

		while((reader.next(key) != null)) {

			if(rows != -1 && count == rows) {
				break;
			}

			if(value != null) {
				reader.getCurrentValue(value);
			}

			if(value != null) {
				System.out.println(stringfy(key) + "\t" + stringfy(value));
			} else {
				System.out.println("key=" + stringfy(key));
			}

			count++;
		}
		System.err.println("done.");

		reader.close();
	}

	private String stringfy(Object o) {
		if(o == null)
			return "#this value was null#";
		if(o instanceof TBase) {
			return ThriftUtils.toJSON((TBase) o);
		} else {
			return o.toString();
		}
	}

	/**
	 * To run just send the file path as the first arg.
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String args[]) throws Exception {
		BaseJob.main(DumpSequenceFileAsText.class, args);
	}

	@Override
	public double getProgress() throws Exception {
		return 0;
	}

	@Override
	public void cancel() throws Exception {
	}

	@Override
	public Properties getJobGeneratedProperties() {
		return new Properties();
	}

}
