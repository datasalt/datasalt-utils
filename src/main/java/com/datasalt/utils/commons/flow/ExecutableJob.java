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


import org.apache.hadoop.conf.Configuration;

import com.datasalt.utils.commons.BaseHadoopJob;



public class ExecutableJob<T extends BaseHadoopJob> implements Executable<Configuration>{

	Class<T> jobClass;
	String[] args;
	
	public ExecutableJob(Class<T> job, String[] args) {
		this.jobClass = job;
		this.args = args;
	}
	
	@Override
  public void execute(Configuration conf) throws Exception {
		T job = jobClass.newInstance();
		job.getJob(args, conf).waitForCompletion(true);
  }
}
