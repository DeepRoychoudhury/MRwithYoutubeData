package com.mr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SecondYoutubeReducer extends MapReduceBase implements Reducer<Text,Text,Text,Long>{
	HashMap<Text,Integer> cache = new HashMap<Text,Integer>();
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Long> output,
		Reporter reporter) throws IOException {
		System.out.println(key);
		long sum=0;
		 while (values.hasNext()) {  
			String number=values.next().toString();
			System.out.println("Number : "+number);
			if (!number.equals("views") && !number.isEmpty()){
			long num = Long.parseLong(number); 
			sum+=num;    
			}
		}		
		//cache.put(key, sum);
		System.out.println("Key : "+key + " Sum : "+sum);
		
		output.collect(key, sum);
	}
	}
