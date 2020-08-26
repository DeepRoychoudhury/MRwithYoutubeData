package com.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SecondYoutubeReducer extends MapReduceBase implements Reducer<Text,Text,Text,Long>{
	HashMap<String,Long> cache = new HashMap<String,Long>();
	ArrayList<Long> med = new ArrayList<Long>();
	Long median = 0L;
	int count=0;
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Long> output,
		Reporter reporter) throws IOException {
		System.out.println(key);
		Long sum=0L;
		 while (values.hasNext()) {  
			String number=values.next().toString();
			System.out.println("Number : "+number);
			if (!number.equals("views") && !number.isEmpty()){
			long num = Long.parseLong(number); 
			sum+=num;
			}		
		}		 

			med.add(sum);
			count++;
		//cache.put(key.toString(), sum);
		System.out.println("Key : "+key + " Sum of views : "+sum);
		//sorting in ascending order
		Collections.sort(med);
		//median calculation
		if(count>1) {
			System.out.println(count);
		if(count % 2 == 0 && count > 2) {
			System.out.println(med.get((count/2)-2) + " : " +med.get((int) (((count/2)-1))));
			if(((med.get((count/2)-2)+med.get((count/2)-1))/2.0)>=0) {
			long alg = (long) ((med.get((count/2)-2)+med.get((count/2)-1))/2.0);
			System.out.println(alg);
			if(alg>=0) {
			median=alg;
			}
			}
		}
		else{
			if(((count/2))>=0) {
			median=med.get(((count/2))-1);
			}
		}
		}
		System.out.println(med);
		System.out.println("Median value is : "+median);
		output.collect(key, sum);
	}
	}
