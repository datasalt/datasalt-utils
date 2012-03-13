package com.datasalt.utils.commons.io;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
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
			long numSecs = (partialEnd-partialStart)/1000;
			log.info(partialCount +" pairs copied in " + numSecs + " secondss => (" + partialCount/numSecs + " paris/s)"); 
			reader.close();
			totalCount+=partialCount;
		}
		long totalEnd = System.currentTimeMillis();
		long numSecs = ((totalEnd-totalStart))/1000;
		log.info("TOTAL"+totalCount +" pairs copied in " + numSecs/60.0f + " minutes => (" + totalCount/numSecs + " pairs/sec)");
		writer.close();
	}
}
