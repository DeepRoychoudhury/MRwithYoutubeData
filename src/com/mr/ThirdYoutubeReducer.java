package com.mr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ThirdYoutubeReducer extends MapReduceBase implements Reducer<Text,Text,Text,Long>{
	HashMap<Text,Integer> cache = new HashMap<Text,Integer>();
	private Text likes = new Text();
	private Text dislikes = new Text();
	private Text comment_count = new Text();
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Long> output,
		Reporter reporter) throws IOException {
		System.out.println(key);
		long likesum = 0,dislikesum = 0,commentsum=0;
		 while (values.hasNext()) {  
			String number=values.next().toString();
			System.out.println("Number : "+number);
			if (number.contains("likes") && !number.contains("dis") && !number.contains(":likes")){
				String splittednum=number.replace("likes:", "");
			long num = Long.parseLong(splittednum); 
			likesum+=num;    
			likes.set(key+" : likes");
			}
			else if(number.contains("dislikes") && !number.contains(":dislikes")) {
				String splittednum=number.replace("dislikes:", "");
				long num = Long.parseLong(splittednum); 
				dislikesum+=num; 
				dislikes.set(key+" : dislikes");
			}
			else if(number.contains("comment_count") && !number.contains(":comment_count")) {
				String splittednum=number.replace("comment_count:", "");
				long num = Long.parseLong(splittednum); 
				commentsum+=num; 			
				comment_count.set(key+" : comment_count");
			}
		}		
		//cache.put(key, sum);
		System.out.println("Key : "+key + "Like Sum : "+likesum+ "Like Sum : "+dislikesum+ "Like Sum : "+commentsum);
		
		output.collect(likes, likesum);
		output.collect(dislikes, dislikesum);
		output.collect(comment_count, commentsum);
	}
	}
