package com.manning.hip.ch3.seqfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.manning.hip.ch3.StockPriceWritable;

public class SequenceFileStockMapReduce {

	public static void main(String[] args) throws Exception {
		String input = "/home/ym/ytmp/output/2";
		String output = "/home/ym/ytmp/output/3";
		runJob(input, output);
	}
	
	public static void runJob(String input, String output) throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(SequenceFileStockMapReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StockPriceWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
		
		FileInputFormat.setInputPaths(job, new Path(input));
		Path outPath = new Path(output);
		FileOutputFormat.setOutputPath(job, outPath);
		outPath.getFileSystem(conf).delete(outPath, true);
		
		job.waitForCompletion(true);
	}
	
}



























