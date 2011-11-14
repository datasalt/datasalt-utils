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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;



/**
 * <p>Base class for jobs. By implementing this class we can run any job 
 * from the command line and from Azkaban. Implement this BaseJob when you want
 * a compatible task with Azkaban, that is injected, and that provides a 
 * {@link Configuration} useful for accessing the HDFS. If what you want
 * is to implement a Hadoop Job, better use {@link BaseHadoopJob}</p>
 * 
 * @author pere
 *
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public abstract class BaseJob {
	
	public abstract double getProgress() throws Exception;
	
	public abstract void cancel() throws Exception;
	
	public abstract void execute(String args[], Configuration conf) throws Exception;
		
	/**
	 * Return Properties to Azkaban
	 * 
	 */
	public abstract Properties getJobGeneratedProperties();

	public static Class<? extends BaseJob> getClass(String className) throws ClassNotFoundException {
		Class cl;
		try {
			/*
			 * Instantiate Job by reflection
			 */
			cl = Class.forName(className);
		} catch(ClassNotFoundException e) {
			cl = Class.forName(className, true, Thread.currentThread().getContextClassLoader());
		}
		if(!BaseJob.class.isAssignableFrom(cl)) {
			throw new RuntimeException("Class is not of type BaseJob");
		}
		return cl;
	}
	
	
	/**
	 * Executes whichever BaseJob. Canonical Class nam comming as first parameter . 
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		/*
		 * Parse arguments like -D mapred. ... = ...
		 */
		
		BaseConfigurationFactory factory = BaseConfigurationFactory.getInstance();
		Configuration conf = factory.getConf();
		
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
	  String[] arguments = parser.getRemainingArgs();
	  BaseJob job = BaseJob.getClass(arguments[0]).newInstance();
		
		//TODO Add log of execution start.
		job.execute(new ArrayList<String>(Arrays.asList(arguments)).subList(1, arguments.length).toArray(new String[0]), conf);
	}

	/**
	 * Main to be called by each individual Job main, just a wrapper 
	 * to the regular main that provides the class name.
	 * @throws Exception 
	 */
	public static void main(Class<? extends BaseJob> jobClass, String args[]) throws Exception {
		ArrayList<String> largs = new ArrayList<String>(Arrays.asList(args));
		largs.add(0, jobClass.getCanonicalName());
		main(largs.toArray(new String[0]));
	}
	

}