package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class InteractiveYoutubeMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text>{
	private Text data = new Text();
	private Text views = new Text();
	private String country;
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		String[] columns = value.toString().split(",");
    	int i=0;
    	for(String column:columns) {
    		
    	 if(i==4) {	
    		 if(column.toString().equals("IN")||column.toString().equals("US")||column.toString().equals("CA")||column.toString().equals("FR")||column.toString().equals("RU")) {
  				country = column;
  			}
    		 else {
    			 if(country!=null) {
    		 if(!column.equals("category_id")) {
    	 data.set(new Text(column+country));
    		 }
    			 }
    		 }
    	 }
    	 if(i==8) {
    		 if(country!=null) {
    		 views.set("likes:"+column);
        	 //views.set((int) Long.parseLong(column));
             output.collect(data, views);
    		 }
    	 }
    	 if(i==9) {
    		 if(country!=null) {
    		 views.set("dislikes:"+column);
        	 //views.set((int) Long.parseLong(column));
             output.collect(data, views);
    		 }
    	 }
    	 if(i==10) {
    		 if(country!=null) {
    		 views.set("comment_count:"+column);
        	 //views.set((int) Long.parseLong(column));
             output.collect(data, views);
    		 }
    	 }
         i++;
    	}	
    	
	}

}
