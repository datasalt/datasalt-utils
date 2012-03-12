package com.datasalt.utils.commons.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileCat {

	
	private static final String HELP="<input_1> <input_2> ... <output>";
	
	
	private static void raiseException(String e) throws IOException {
		throw new IOException(e);
	}
	
	private static void checkKeyValue(FileSystem fs,Configuration conf,List<Path> inputs) 
			throws 	IOException {
		Path firstInput = inputs.get(0);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs,firstInput, conf);
		Class keyClass = reader.getKeyClass();
		Class valueClass = reader.getValueClass();
		reader.close();
		
		for (int i=1 ; i < inputs.size() ; i++){
			Path input = inputs.get(i);
			reader = new SequenceFile.Reader(fs,input, conf);
			Class currentKey = reader.getKeyClass();
			Class currentValue = reader.getValueClass();
			if (currentKey != keyClass){
				raiseException("Key class must be the same in all files.File '" + "' keyClass="+currentKey);
			}
			if (currentValue != valueClass){
				raiseException("Key class must be the same in all files.File '" + "' keyClass="+currentValue);
			}
			reader.close();
		}
	}
	
	public static void main(String[] args) throws IOException{
		if (args.length < 2){
			System.err.println("At least one input and output");
			System.err.println(HELP);
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		List<Path> inputs = new ArrayList<Path>();
		for (int i=0 ; i < args.length -1 ; i++){
			inputs.add(new Path(args[i]));
		}
		Path output = new Path(args[args.length-1]);
		concat(fs,conf,inputs,output);
	}
	
	
	public static void concat(FileSystem fs,Configuration conf,List<Path> inputs,Path output) throws IOException{
		checkKeyValue(fs,conf,inputs);
		
		Path firstInput = inputs.get(0);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs,firstInput, conf);
		Class keyClass = reader.getKeyClass();
		Class valueClass = reader.getValueClass();
		reader.close();
		
		SequenceFile.Writer writer = 
				new SequenceFile.Writer(fs,conf,output,keyClass,valueClass);
		
		//TODO add compression, and blabla!
		//What about the header ?
		Object key = ReflectionUtils.newInstance(keyClass,null);
		Object value = ReflectionUtils.newInstance(valueClass,null);
		for (Path input : inputs){
			reader = new SequenceFile.Reader(fs,input, conf);
			while (reader.next(key) != null){
						value = reader.getCurrentValue(value);
						writer.append(key, value);
			}
			reader.close();
		}
		writer.close();
	}
}
