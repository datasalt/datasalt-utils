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

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * This mapper can be used in {@link MultiJoiner} jobs that must emit more than one channel from the same mapper. The
 * user has to use the methods in MultiJoiner API that accept such mapper implementations in order to add them to the
 * joiner specification.
 * 
 * @author pere
 * 
 * @param <INPUT_KEY>
 * @param <INPUT_VALUE>
 */
@SuppressWarnings({ "rawtypes" })
public class MultiJoinMultiChannelMapper<INPUT_KEY, INPUT_VALUE> extends MultiJoinMapperBase<INPUT_KEY, INPUT_VALUE> {

	/*
	 * The following methods can be used as a shortcut for emit(Object, SS, T, channel)
	 */
	protected void emit(String grouping, Object datum, int channel) throws IOException, InterruptedException {
		byte[] array = grouping.getBytes("UTF-8");
		emitBytes(array, 0, array.length, null, datum, channel);
	}

	protected void emit(Object grouping, Object datum, int channel) throws IOException, InterruptedException {
		byte[] array = ser.ser(grouping);
		emitBytes(array, 0, array.length, null, datum, channel);
	}

	protected void emit(Text grouping, WritableComparable secondarySort, Object datum, int channel) throws IOException,
	    InterruptedException {
		emitBytes(grouping.getBytes(), 0, grouping.getLength(), secondarySort, datum, channel);
	}

	protected void emit(Text grouping, Object datum, int channel) throws IOException, InterruptedException {
		emitBytes(grouping.getBytes(), 0, grouping.getLength(), null, datum, channel);
	}

	protected void emit(Object grouping, WritableComparable secondarySort, Object datum, int channel) throws IOException,
	    InterruptedException {
		byte[] array = ser.ser(grouping);
		emitBytes(array, 0, array.length, secondarySort, datum, channel);
	}
}
