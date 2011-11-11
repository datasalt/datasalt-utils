package com.datasalt.utils.commons.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.datasalt.utils.commons.PangolinConfigurationFactory;
import com.datasalt.utils.io.Serialization;

public class PangolinBaseTest {

	
	
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
			conf =PangolinConfigurationFactory.getInstance().getConf(); 
		}
		return conf;
	}

	
	
	
}