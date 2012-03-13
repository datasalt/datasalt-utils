package com.datasalt.utils.commons.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

public class SequenceFileCat {

	private static final Logger log = Logger.getLogger(SequenceFileCat.class);
	
	private FileSystem fs;
	private Configuration conf;
	private List<Path> inputs;
	private Path output;
	
	public SequenceFileCat(){
		
	}
	
	public void setFileSystem(FileSystem fs){
		this.fs = fs;
	}
	
	public void setConf(Configuration conf){
		this.conf = conf;
	}
	
	public List<Path> getInputs() {
		return inputs;
	}

	public void setInputs(List<Path> inputs) {
		this.inputs = inputs;
	}

	public Path getOutput() {
		return output;
	}

	public void setOutput(Path output) {
		this.output = output;
	}

	public Configuration getConf() {
		return conf;
	}

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
			Metadata metadata = reader.getMetadata();
			CompressionCodec codec = reader.getCompressionCodec();
			log.info("Input:" + input);
			log.info("Metadata:"+metadata);
			log.info("Compression:"+codec);
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
	
	public static void concat(FileSystem fs,Configuration conf,List<Path> inputs,Path output) throws IOException{
		SequenceFileCat o = new SequenceFileCat();
		o.setFileSystem(fs);
		o.setConf(conf);
		o.setInputs(inputs);
		o.setOutput(output);
		o.concat();
	}
	
	public void concat() throws IOException {
		checkKeyValue(fs,conf,inputs);
		
		Path firstInput = inputs.get(0);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs,firstInput, conf);
		Class keyClass = reader.getKeyClass();
		Class valueClass = reader.getValueClass();
		log.info("Key:"+keyClass +"  Value:"+valueClass);
		reader.close();
		
		SequenceFile.Writer writer = 
				new SequenceFile.Writer(fs,conf,output,keyClass,valueClass);
		log.info("Output compression : " + writer.getCompressionCodec());
		
		//TODO add compression, and blabla!
		//What about the header ?
		Object key = ReflectionUtils.newInstance(keyClass,null);
		Object value = ReflectionUtils.newInstance(valueClass,null);
		int totalCount=0;
		int partialCount=0;
		long totalStart=System.currentTimeMillis();
		for (Path input : inputs){
			reader = new SequenceFile.Reader(fs,input, conf);
			log.info("Copying input:"+input);
			long partialStart=System.currentTimeMillis();
			partialCount=0;
			while (reader.next(key) != null){
						value = reader.getCurrentValue(value);
						writer.append(key, value);
						partialCount++;
			}
			long partialEnd = System.currentTimeMillis();
			double numSecs = (partialEnd-partialStart)/1000.0;
			double avg = (numSecs == 0) ? Double.NaN : partialCount / numSecs;
			log.info(partialCount +" pairs copied in " + numSecs + " secondss => (" + avg + " pairs/s)"); 
			reader.close();
			totalCount+=partialCount;
		}
		long totalEnd = System.currentTimeMillis();
		double numSecs = ((totalEnd-totalStart))/1000.0;
		double avg = (numSecs == 0) ? Double.NaN : totalCount / numSecs;
		log.info("TOTAL:"+totalCount +" pairs copied in " + (int)((numSecs)/60) + " min " + ((int)numSecs%60) + " sec  => (" + avg + " pairs/sec)");
		writer.close();
	}
	private static final String HELP="<input_1> <input_2> ... <output>";
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
			Path p = new Path(args[i]);
			FileStatus[] statuses = fs.globStatus(p);
			for (FileStatus s : statuses){
				Path sP = s.getPath();
				System.out.println("Input:" + sP);
				inputs.add(sP);
			}
		}
		Path output = new Path(args[args.length-1]);
		System.out.println("Output:" + output);
		com.datasalt.utils.commons.io.SequenceFileCat.concat(fs,conf,inputs,output);
	}
	
	
	
}
