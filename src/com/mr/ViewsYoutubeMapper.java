package com.mr;

/*import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Mapper;*/
//import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ViewsYoutubeMapper extends MapReduceBase implements Mapper<Object, Text, Text, DoubleWritable> {
	Text data = new Text();
	DoubleWritable views;

	@Override
	public void map(Object key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {
		String[] columns = value.toString().split(",");
    	int i=0;
    	for(String column:columns) {
    	 if(i==4) {	
    		 if(!column.contains("category_id")) {
    	 data.set(column);
    		 }
    	 }
    	 if(i==7) {
    		 if (!column.contains("views") && !column.isEmpty()) {
    		 views = new DoubleWritable(Double.parseDouble(column));
             output.collect(data, views);
    		 }
    	 }
         i++;
    	}			
	}
}
