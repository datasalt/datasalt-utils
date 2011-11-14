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

import org.apache.hadoop.io.WritableComparable;

import com.datasalt.utils.io.Serialization;
import com.datasalt.utils.mapred.BaseMapper;

/**
 * This is the base abstract class that can be used to implement mappers for {@link MultiJoiner} jobs. It contains all
 * the base logic that uses {@link MultiJoinPair} for the key and {@link MultiJoinDatum} for the values. It uses the
 * hadoop serialization ({@link Serialization} to convert objects into byte[].
 * <p>
 * See {@link MultiJoinMultiChannelMapper} for a mapper that can emit more than one channel from the same mapper and
 * {@link MultiJoinChanneledMapper} for a mapper that can be configured to use always the same channel.
 * 
 * @author pere
 * 
 * @param <INPUT_KEY>
 * @param <INPUT_VALUE>
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class MultiJoinMapperBase<INPUT_KEY, INPUT_VALUE> extends BaseMapper<INPUT_KEY, INPUT_VALUE, MultiJoinPair, MultiJoinDatum> {

	protected MultiJoinPair key;
	protected MultiJoinDatum datum = new MultiJoinDatum();
	protected Context context;

	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		this.context = context;
		try {
			this.key = (MultiJoinPair) Class.forName(context.getConfiguration().get(MultiJoiner.MULTIJOINER_KEY_IMPL))
			    .newInstance();
		} catch(Exception e) {
			throw new IOException(e);
		}
	};

	protected void emitBytes(byte[] grouping, int offset, int length, WritableComparable secondarySort, Object datum,
	    int channel) throws IOException, InterruptedException {

		key.setMultiJoinGroup(grouping, offset, length);
		key.setChannelId(channel);
		if(secondarySort != null) {
			key.setSecondSort(secondarySort);
		}
		this.datum.setDatum(ser.ser(datum));
		this.datum.setChannelId(channel);
		context.write(key, this.datum);
	}
}
