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

package com.datasalt.utils.mapred.joiner;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparator;

/**
 * {@link MultiJoinPair} for having custom secondary sorting in 
 * the {@link MultiJoiner}. Use method {@link MultiJoiner#setMultiJoinPairClass(Class)}
 * for configuring a {@link MultiJoiner} job with this class.
 * <p>
 * With this class we can secondary-sort ascendantly by a double. 
 * 
 * @author pere
 */
public class MultiJoinPairDouble extends MultiJoinPair<DoubleWritable>{

	public MultiJoinPairDouble() throws InstantiationException, IllegalAccessException {
	  super(DoubleWritable.class);
  }
	
	public static class Comparator extends MultiJoinPair.Comparator {

		public Comparator() {
	    super(DoubleWritable.class);
    }
	}
	
	static {
		WritableComparator.define(MultiJoinPairDouble.class, new Comparator());
	}
}

