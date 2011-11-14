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

package com.datasalt.utils.viewbuilder;

import java.io.File;

import org.apache.log4j.Logger;
import org.apache.solr.cloud.ZkController;

/**
 * This manager allows to perform simple operations to the Zookeeper server that stores the SolrCloud configuration.
 * Basically it provides methods to store a solr configFile in Zookeeper, and assign it to several collections(Solr Cores..).
 * 
 * @author epalace
 */
public class SolrCloudManager {

	private static final Logger log = Logger.getLogger(SolrCloudManager.class);
	
	public static void printHelp(String error){
		String HELP = "Params:\n"+
									"[host:port] --upload-configs ([config_dir] [config_name])* \n"+
									"[host:port] --set-config-names ([config_name] [collection1])* \n";
		if (error != null){
			System.out.println("Error: " + error);
		}
		System.out.println(HELP);
		System.exit(1);
	}
	
	public static void main(String[] args) {
		try {
		if (args.length < 2){
			printHelp(null);
		}
		String zkServerAddress=args[0];
		int zkClientTimeout = 1000;
		int zkClientConnectTimeout=1000;
		String localHost=null;
		String locaHostPort=null;
		String localHostContext=null;
		
		if ("--upload-configs".equals(args[1])){
			
			if (args.length < 4){
				printHelp("Not enough arguments");
			}
			ZkController controller = new ZkController(zkServerAddress, zkClientTimeout, zkClientConnectTimeout, localHost, locaHostPort, localHostContext);
			
			for (int i=2; i < args.length ; i+=2){
				File configDir=new File(args[i]);
				String configName=args[i+1];
				controller.uploadConfigDir(configDir, configName);
				log.info("Config dir [" + configDir + "] succesfully stored in configName [" + configName + "]");
			}
		} else if ("--set-config-names".equals(args[1])){
			if (args.length < 4){
				printHelp("Not enough arguments");
			}
			ZkController controller = new ZkController(zkServerAddress, zkClientTimeout, zkClientConnectTimeout, localHost, locaHostPort, localHostContext);
			
			for (int i=2 ; i < args.length ; i+=2){
				String configName=args[i];
				String collection=args[i+1];
				controller.setConfignameToCollection(configName, collection);
				log.info("Config [" + configName + "] succesfully set in collection [" + collection + "]");
			}
		} else {
			printHelp(args[1] +  " is not a valid command");
		}
		} catch(Exception e){
			e.printStackTrace();
			System.exit(1);
		}
	}
}
