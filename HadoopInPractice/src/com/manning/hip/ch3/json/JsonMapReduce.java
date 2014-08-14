package com.manning.hip.ch3.json;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonMapReduce {
	private static final Logger log = LoggerFactory.getLogger(JsonMapReduce.class);
	
	public static class MappClass extends Mapper<LongWritable, MapWritable, Text, Text>{
		@Override
		protected void map(LongWritable key, MapWritable value, Context context)
				throws IOException, InterruptedException {
			for (Entry<Writable, Writable> entry : value.entrySet()){
				context.write((Text)entry.getKey(), (Text)entry.getValue());
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		String input = new String("file:///home/ym/ytmp/data/json/");
		String output = new String("file:///home/ym/ytmp/output/1");
		runJob(input, output);
	}
	
	public static void runJob(String input, String output) throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(JsonMapReduce.class);
		
		job.setMapperClass(MappClass.class);
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
				
		job.setInputFormatClass(JsonInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(input));
		Path outPath = new Path(output);
		FileOutputFormat.setOutputPath(job, outPath);
		outPath.getFileSystem(conf).delete(outPath, true);
		
		job.waitForCompletion(true);
	}
}
