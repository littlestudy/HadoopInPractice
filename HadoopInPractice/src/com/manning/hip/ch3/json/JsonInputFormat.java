package com.manning.hip.ch3.json;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.manning.hip.common.HadoopCompat;

public class JsonInputFormat extends FileInputFormat<LongWritable, MapWritable>{

	private static final Logger log = LoggerFactory.getLogger(JsonInputFormat.class);
	
	@Override
	public RecordReader<LongWritable, MapWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new JsonRecordReader();
	}
	
	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		CompressionCodec codec = new CompressionCodecFactory(HadoopCompat.getConfiguration(context)).getCodec(file);
		return codec == null;
	}
	
	public static class JsonRecordReader extends RecordReader<LongWritable, MapWritable>{
		private static final Logger LOG = LoggerFactory.getLogger(JsonRecordReader.class);
		
		private LineRecordReader reader = new LineRecordReader();
		
		//private final Text currentLine = new Text();
		private final MapWritable value_ = new MapWritable();
		private final JSONParser jsonParser_ = new JSONParser();
		
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			reader.initialize(split, context);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			while (reader.nextKeyValue()){
				value_.clear();
				if (decodeLineToJson(jsonParser_, reader.getCurrentValue(), value_))
					return true;
			}
			return false;
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return reader.getCurrentKey();
		}

		@Override
		public MapWritable getCurrentValue() throws IOException,
				InterruptedException {
			return value_;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return reader.getProgress();
		}

		@Override
		public void close() throws IOException {
			reader.close();			
		}
		
		public static boolean decodeLineToJson(JSONParser parser, Text line, MapWritable value){
			log.info("Got string '{}'", line);
			try {
				JSONObject jsonObject = (JSONObject)parser.parse(line.toString());
				for (Object key : jsonObject.keySet()){
					Text mapKey = new Text(key.toString());
					Text mapValue = new Text();
					if (jsonObject.get(key) != null){
						mapValue.set(jsonObject.get(key).toString());
					}
					value.put(mapKey, mapValue);
				}
				return true;
			} catch (ParseException e) {
				LOG.warn("Could not json-decode string: " + line, e);
		        return false;
			} catch (NumberFormatException e){
		        LOG.warn("Could not parse field into number: " + line, e);
		        return false;
			}
		}
	}

}
