package com.mr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class YoutubeReducer extends MapReduceBase implements Reducer<Text,Text,Text,IntWritable>{

	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, IntWritable> output,
		Reporter reporter) throws IOException {
		System.out.println(key);
		int sum=0;
		 while (values.hasNext()) {  
			String number=values.next().toString();
			int num = Integer.parseInt(number); 
			sum+=num;    
		}		
		output.collect(key, new IntWritable(sum));
	}
	}

