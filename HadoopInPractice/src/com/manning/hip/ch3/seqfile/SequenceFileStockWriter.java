package com.manning.hip.ch3.seqfile;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;

import com.manning.hip.ch3.StockPriceWritable;

public class SequenceFileStockWriter {
	public static void main(String[] args) throws IOException {
		String input = "/home/ym/ytmp/data/seqfile/stocks.txt";
		String output = "/home/ym/ytmp/output/2";
		write(new File(input), new Path(output));
	
	}
	
	public static void write(File inputFile, Path outputPath) throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf
				, outputPath, Text.class, StockPriceWritable.class
				, SequenceFile.CompressionType.BLOCK
				, new DefaultCodec());
		
		try {
			Text key = new Text();
			
			for (String line: FileUtils.readLines(inputFile)){
				StockPriceWritable stock = StockPriceWritable.fromLine(line);
				key.set(stock.getSymbol());
				//key.set(stock.getSymbol());
				writer.append(key, stock);
			}
		} finally {
			writer.close();
		}
	}
}
