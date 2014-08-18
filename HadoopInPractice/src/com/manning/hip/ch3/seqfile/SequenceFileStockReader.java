package com.manning.hip.ch3.seqfile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;

import com.manning.hip.ch3.StockPriceWritable;

public class SequenceFileStockReader {
	public static void main(String[] args) throws IOException {
		String input = "/home/ym/ytmp/output/2";
		read(new Path(input));
	}
	
	public static void read(Path inputPath) throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		SequenceFile.Reader reader = new Reader(fs, inputPath, conf);
		try {
			System.out.println("Is block compressed = " + reader.isBlockCompressed());
			
			Text key = new Text();
			StockPriceWritable value = new StockPriceWritable();
			
			while (reader.next(key, value)){
				System.out.println(key + "," + value);				
			}
		}finally{
			reader.close();
		}
	}
}
