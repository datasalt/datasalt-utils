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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;



import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.util.NamedList;
import org.apache.velocity.exception.ParseErrorException;


public class SolrAdminCoreUtils {
	
	/**
	 * 
	 */
	public static NamedList<Object> createNewCore(URL adminUrl,String coreName,String instanceDir) throws SolrServerException, IOException {
		SolrServer adminServer = new CommonsHttpSolrServer(adminUrl);
		return createNewCore(adminServer,coreName,instanceDir);
	}
	
	public static NamedList<Object> createNewCore(SolrServer adminServer, String coreName,String instanceDir) throws SolrServerException, IOException{
		CoreAdminRequest.Create req = new CoreAdminRequest.Create();
		req.setCoreName(coreName);
		req.setInstanceDir(instanceDir);
		
		return adminServer.request(req);
	}
	
	
	public static NamedList<Object> createNewCore(SolrServer adminServer, String coreName,String instanceDir,String dataDir,String schemaName,String configName) throws SolrServerException, IOException{
		CoreAdminRequest.Create req = new CoreAdminRequest.Create();
		req.setConfigName(configName);
		req.setSchemaName(schemaName);
		req.setDataDir(dataDir);
		req.setCoreName(coreName);
		req.setInstanceDir(instanceDir);
		return adminServer.request(req);
	}
	
	public static NamedList<Object> hotSwapCores(URL adminUrl,String coreName,String otherCoreName) throws SolrServerException, IOException {
		SolrServer adminServer = new CommonsHttpSolrServer(adminUrl);
		return hotSwapCores(adminServer,coreName,otherCoreName);
	}
	
	public static NamedList<Object> hotSwapCores(SolrServer adminServer,String coreName,String otherCoreName) throws SolrServerException, IOException {

		CoreAdminRequest aReq = new CoreAdminRequest();
		aReq.setAction(CoreAdminAction.SWAP);
		aReq.setCoreName(coreName);
		aReq.setOtherCoreName(otherCoreName);
		return adminServer.request(aReq);
	}
		
	public static NamedList<Object> unloadCore(SolrServer adminServer,String coreName,boolean deleteIndex) throws SolrServerException, IOException {
		CoreAdminRequest.Unload req = new CoreAdminRequest.Unload(deleteIndex);
		req.setCoreName(coreName);
		return adminServer.request(req);
	}
	
	public static NamedList<Object> unloadCore(URL adminUrl,String coreName,boolean deleteIndex) throws SolrServerException, IOException {
		SolrServer adminServer = new CommonsHttpSolrServer(adminUrl);
		return unloadCore(adminServer,coreName,deleteIndex);
	}
	
	/**
	 * It expects something like :   http://shard1_replica1:8500/solr|http://shard1_replica2:8501/solr,shard2_replica1:8502/solr|http://shard2_replica2:8503/solr,  ...
	 * @param csvShardReplicas
	 * @return
	 * @throws MalformedURLException
	 */
	public static List<URL[]> parseShardReplicas(String csvShardReplicas) throws MalformedURLException{
		
		String[] shards = csvShardReplicas.split(",");
		
		List<URL[]> solrShards = new ArrayList<URL[]>();
		for (String shard : shards){
			String[] replicas = shard.split("\\|");
			URL[] replicasUrls = new URL[replicas.length];
			int i = 0 ; 
			for(String replica : replicas) {
				URL newUrl = new URL(replica);
				for(URL[] existingShards : solrShards) {
					for(URL existingReplica : existingShards){
						if (existingReplica.equals(newUrl)){
						throw new ParseErrorException("Repeated solr admin url in properties [" + existingReplica + "]");
						}
					}
				}
				replicasUrls[i++] = newUrl; 
			}
			solrShards.add(replicasUrls);
		}
		return solrShards;
	}
	
	/**
	 * Extracts the core names using the subfolder names of the specified folder
	 * @param fs
	 * @param coreFolder
	 * @return
	 * @throws IOException
	 */
	public static List<String> findCoresToDeploy(FileSystem fs,Path coreFolder) throws IOException{
		FileStatus[] indexes = fs.globStatus(new Path(coreFolder+"/*"),new IsDirFilter(fs));
		List<String> coreArray = new ArrayList<String>();
		for (FileStatus index : indexes){
			coreArray.add(index.getPath().getName());
		}
		return coreArray;
	}
	
	
	/**
	 * Filter used in globStatus to filter just folders
	 */
	public static final class IsDirFilter implements PathFilter{
		private FileSystem fs;
		public IsDirFilter(FileSystem fs){
			this.fs = fs;
		}
		public boolean accept(Path path) { try {
      return  fs.getFileStatus(path).isDir();
    } catch(IOException e) {
      return false;
    } }
	}
	
	
	
	
}
