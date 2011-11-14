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

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An Executable with many childs. Each child is supposed to be
 * executed in pararell, but this implementation executes them
 * in sequential order. 
 * 
 * @author ivan
 *
 * @param <ConfigData>
 */
public class ChainableExecutable<ConfigData> implements Executable<ConfigData> {
	
  static Logger log = LoggerFactory.getLogger(ChainableExecutable.class);
	
	Executable<ConfigData> executable;
	ArrayList<Executable<ConfigData>> childs = new ArrayList<Executable<ConfigData>>();
	String name;
	String description;
	ChainableExecutable<ConfigData> parent;
	
	public ChainableExecutable(String name, String description, ChainableExecutable<ConfigData> father, Executable<ConfigData> work) {
		this.name = name;
		this.executable = work;
		this.description = description;
		parent = father;
		if (father != null) {
			father.registerChild(this);
		}
	}

	@Override
  public void execute(ConfigData configData) throws Exception {
		
		if (executable != null) {
			log.info("Executing [" + name + "]");
			executable.execute(configData);
		}
		for (Executable<ConfigData> e : childs) {
			e.execute(configData);
		}
  }
	
	protected GvizNode genGviz() {
		GvizNode ret = new GvizNode();
		String tail = getName();
		for (Executable<ConfigData> e : childs) {
			if (e instanceof ChainableExecutable) {				
				ChainableExecutable<?> c = (ChainableExecutable<?>) e;
				
				ret.edges.add(getName() +" -> " + c.getName());
				
				GvizNode n = c.genGviz();
				if (tail == getName()) {
					tail = n.tail;
				}
				ret.edges.addAll(n.edges);
			}			
		}
		ret.head = getName();
		ret.tail = tail;
		return ret;
	}
	
	public void registerChild(Executable<ConfigData> executable) {
		childs.add(executable);
	}

	public String getName() {
  	return name;
  }

	public String getDescription() {
  	return description;
  }

	public ChainableExecutable<ConfigData> getParent() {
  	return parent;
  }
		
}
