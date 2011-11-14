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

package com.datasalt.utils.commons.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.datasalt.utils.commons.BaseConfigurationFactory;
import com.datasalt.utils.io.Serialization;

public class BaseTest {

	
	
	//public final static TypeReference<HashMap<String, Object>> MAP = new TypeReference<HashMap<String, Object>>() {
	//};
	//protected ObjectMapper mapper = new ObjectMapper();

	private Configuration conf;
	protected Serialization ser; 
	

	public Serialization getSer() throws IOException {
		if (ser == null) {
			ser = new Serialization(getConf());	
		}
		return ser;
	}

	public Configuration getConf() throws IOException {
		if (conf == null){
			conf =BaseConfigurationFactory.getInstance().getConf(); 
		}
		return conf;
	}

	
	
	
}