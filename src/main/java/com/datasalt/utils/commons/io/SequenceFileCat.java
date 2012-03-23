package com.datasalt.utils.commons.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
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
	private Metadata metadata;
	public Metadata getMetadata() {
		return metadata;
	}

	public void setMetadata(Metadata metadata) {
		this.metadata = metadata;
	}

	public FileSystem getFs() {
		return fs;
	}

	

	public CompressionCodec getCodec() {
		return codec;
	}

	public void setCodec(CompressionCodec codec) {
		this.codec = codec;
	}

	public CompressionType getCompressionType() {
		return compressionType;
	}
	private CompressionType compressionType=CompressionType.BLOCK;
	private CompressionCodec codec=new org.apache.hadoop.io.compress.DefaultCodec();
	
	public SequenceFileCat(Configuration conf,FileSystem fs){
		this.conf = conf;
		this.fs = fs;
		
	}
	
	public void setCompressionType(CompressionType compressionType){
		this.compressionType = compressionType;
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
			log.info("Compression:" + codec);
			if (codec != null){
			log.info("Compression:"+codec.getCompressorType() + "," + codec.getDefaultExtension());
			}
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
	
	public void concat() throws IOException {
		checkKeyValue(fs,conf,inputs);
		
		Path firstInput = inputs.get(0);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs,firstInput, conf);
		Class keyClass = reader.getKeyClass();
		Class valueClass = reader.getValueClass();
		log.info("Key:"+keyClass +"  Value:"+valueClass);
		reader.close();
		SequenceFile.Writer writer;
		if (metadata != null){
			writer = SequenceFile.createWriter(fs,conf, output, keyClass, valueClass, compressionType, codec,null,metadata);
		} else {
			writer = SequenceFile.createWriter(fs,conf, output, keyClass, valueClass, compressionType, codec);
		}
		CompressionCodec codec = writer.getCompressionCodec();
		log.info("Output compression : " + codec);
		if (codec != null){
			log.info("Output compression:" + codec.getDefaultExtension() + "," + codec.getCompressorType());
		}
		

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
		writer.close();
		long totalEnd = System.currentTimeMillis();
		double numSecs = ((totalEnd-totalStart))/1000.0;
		double avg = (numSecs == 0) ? Double.NaN : totalCount / numSecs;
		log.info("TOTAL:"+totalCount +" pairs copied in " + (int)((numSecs)/60) + " min " + ((int)numSecs%60) + " sec  => (" + avg + " pairs/sec)");
		
	}
	private static final String HELP="[OPTIONAL --local ] <input_1> <input_2> ... <output>";
	public static void main(String[] args) throws IOException{
		Configuration conf = new Configuration();
		main(args,conf);
	}
	
	public static void main(String[] args,Configuration conf) throws IOException{
		if (args.length < 2){
			System.err.println("At least one input and output");
			System.err.println(HELP);
			System.exit(-1);
		}
		int offset=0;
		FileSystem fs;
		if ("--local".equals(args[0])){
			fs = FileSystem.getLocal(conf);
			offset=1;
		} else {
			fs = FileSystem.get(conf);
		}
		
		List<Path> inputs = new ArrayList<Path>();
		for (int i=offset ; i < args.length -1 ; i++){
			Path p = new Path(args[i]);
			FileStatus[] statuses = fs.globStatus(p);
			for (FileStatus s : statuses){
				Path sP = s.getPath();
				System.out.println("Input:" + sP);
				inputs.add(sP);
			}
		}
		
		if (inputs.isEmpty()){
			System.err.println("No existing inputs ");
			System.exit(-1);
		}
		
		Path output = new Path(args[args.length-1]);
		System.out.println("Output:" + output);
		
		SequenceFileCat s = new SequenceFileCat(conf,fs);
		s.setInputs(inputs);
		s.setOutput(output);
		//TODO update this to enable different compressionCodec and types;
		//s.setCodec(codec);
		//s.setCompressionType(type);
		//s.setMetadata(..)
		s.concat();
	}
	
	public static void printInfo(Path input,Configuration conf) throws IOException{
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs,input, conf);
		CompressionCodec codec = reader.getCompressionCodec();
		Metadata metadata = reader.getMetadata();
		Class keyClass = reader.getKeyClass();
		Class valueClass = reader.getValueClass();
		System.out.println("Compression codec:" + codec.getClass());
		System.out.println("Compressor type:" + codec.getCompressorType());
		System.out.println("Decompressor type:" + codec.getDecompressorType());
		System.out.println("key:"+ keyClass + " value:" + valueClass);
		System.out.println("Metadata:"+metadata);
		reader.close();
	}

	
	
}
