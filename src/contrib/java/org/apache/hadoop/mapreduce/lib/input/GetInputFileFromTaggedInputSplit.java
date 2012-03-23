package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;

public class GetInputFileFromTaggedInputSplit {

	public static String get(InputSplit iS) throws IOException {
		if(iS instanceof TaggedInputSplit) {
			TaggedInputSplit t = (TaggedInputSplit) iS;
			FileSplit fS = (FileSplit) t.getInputSplit();
			return fS.getPath().toString();
		} else if(iS instanceof FileSplit) {
			FileSplit fS = (FileSplit) iS;
			return fS.getPath().toString();
		} else {
			throw new IOException("Unable to get file from unknown InputSplit : " + iS);
		}
		
	}
}
