package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Top10YoutubeMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text>{
	//private final static IntWritable one = new IntWritable(1);  
    private Text data = new Text(); 
    private Text num = new Text();
    private String country ;
   // private IntWritable num = new IntWritable(1);
    public void map(LongWritable key, Text value,OutputCollector<Text,Text> output,     
            Reporter reporter) throws IOException{    
    	//System.out.println(value.toString());
    	num.set("1");
    	String[] columns = value.toString().split(",");
    	int i=0;
    	for(String column:columns) {
    	 if(i==4) {	
    		 if(column.toString().equals("IN")||column.toString().equals("US")||column.toString().equals("CA")||column.toString().equals("FR")||column.toString().equals("RU")) {
    				country = column;
    			}
    		 else {
    			 System.out.println(column+country);
    			 if(country!=null) {
    	 data.set(new Text(column+country)); 
         output.collect(data, num);
    			 }
    		 }    	   	 
    	 }
         i++;
    	}
    }
}
