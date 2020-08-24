package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class YoutubeReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	public void reduce(Text data, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
	{
		
	   for(IntWritable value : values)
	   {
	   con.write(data, new IntWritable(value.get()));
	   }
	}
}
