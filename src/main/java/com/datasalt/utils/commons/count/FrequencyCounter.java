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

package com.datasalt.utils.commons.count;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A helper class for using Map<T, Integer> in a way that we don't need to care about NPEs.
 * 
 * @author pere
 *
 * @param <T>
 */
public class FrequencyCounter<T extends Comparable<T>> extends HashMap<T, Integer> {

	/**
   * 
   */
  private static final long serialVersionUID = 1L;

	public void increment(T key) {
		increment(key, 1);
	}
	
	public void increment(T key, int n) {
		put(key, getCount(key) + n);
	}
	
	public int getCount(T key) {
		Integer count = get(key);
		if(count == null) {
			count = 0;
		}
		return count;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public List<T> getTop(int n) {
		List<T> listToReturn = new ArrayList<T>(n);
		Map.Entry[] entries = entrySet().toArray(new Map.Entry[0]);
		Arrays.sort(entries, new Comparator() {
			@Override
      public int compare(Object o1, Object o2) {
				Map.Entry<T, Integer> arg0 = (Map.Entry<T, Integer>) o1;
				Map.Entry<T, Integer> arg1 = (Map.Entry<T, Integer>) o2;
	      return arg1.getValue().compareTo(arg0.getValue());
      }
		});
		for(int i = 0; i < n && i < entries.length; i++) {
			Map.Entry<T, Integer> entry = entries[i];
			listToReturn.add(entry.getKey());
		}
		return listToReturn;
	}
}
