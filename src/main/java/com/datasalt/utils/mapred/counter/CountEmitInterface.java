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

package com.datasalt.utils.mapred.counter;

import java.io.IOException;



/**
 * Inteface for emiting items to be counted. See {@link MapRedCounter.MapRedCounterMapper} 
 * 
 * @author ivan
 *
 */
public interface CountEmitInterface {
	
	/**
	 * Emits a new Item to be counted. After the execution
	 * of the counter will be present the following stats:<br/>
	 * [typeIdentifier, group, item] -> count <br/>
	 * [typeIdentifier, group] -> count, distinctItemsCount<br/>
	 * <br/>
	 * Also the list of distinct items per group will exist in a file.<br/>
	 * The typeIdentifier is there to be used for identifying
	 * the types of the group and the item. Because in the same file
	 * will be present counts for different groups and items that will 
	 * maybe be of different types, this number can be used to identify
	 * to which one it belongs. 
	 */
	void emit(int typeIdentifier, Object group, Object item) throws IOException, InterruptedException ;

	/**
	 * Same as {@link #emit(int, Object, Object)} but with a previously known number of times to account
	 */
	void emit(int typeIdentifier, Object group, Object item, long nTimes) throws IOException, InterruptedException ;
}
