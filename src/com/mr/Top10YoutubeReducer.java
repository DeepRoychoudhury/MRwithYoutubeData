package com.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Top10YoutubeReducer extends MapReduceBase implements Reducer<Text,Text,Text,IntWritable>{
	HashMap<Integer, String> pairs= new HashMap<Integer,String>();
	HashMap<String,Integer> settingpairs = new HashMap<String,Integer>();
	ArrayList<Integer> sumlength = new ArrayList<Integer>();
	ArrayList<Integer> top10 = new ArrayList<Integer>();
	String country;
	PostgresqlJdbcConnection pg = new PostgresqlJdbcConnection();
	
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, IntWritable> output,
		Reporter reporter) throws IOException {
		System.out.println(key);
		
		//country identifier
		key = countryidentifier(key);
		int sum=0;
		 while (values.hasNext()) {  
			String number=values.next().toString();
			int num = Integer.parseInt(number); 
			sum+=num;    
		}	
		 
		 //Finding out top 10 sums
		 sumlength.add(sum);
		 Collections.sort(sumlength);
		 System.out.println("Sorted Collection of sums : "+sumlength);
		 if(sumlength.size()>10) {
			 top10.clear();
			 for(int i=sumlength.size()-1;i>(sumlength.size()-1)-10;i--) {
				 top10.add(sumlength.get(i));
			 }
			 System.out.println("Top 10 sums are : "+top10);		 
			 
		 }
		 
		 //Finding out top 10 categories key,value pair here from HashMap
		 if(top10.size()==10) {
			 pg.deleteAllDatawithcountry(country);
			 
		 System.out.println("Top 10 category id with Sums are : ");
		 for(int i=0;i<top10.size();i++) {
			 //settingpairs.put(pairs.get(top10.get(i)), top10.get(i));
			 System.out.println(pairs.get(top10.get(i)) + " : " + top10.get(i));
			 System.out.println(country);
			 pg.inserttop10Data(pairs.get(top10.get(i)), top10.get(i), country);
			 //params.setTop10categories(settingpairs);
		 }
		 }
		 if(key!=null) {
		 pairs.put(sum, key.toString());
		 
		 output.collect(key, new IntWritable(sum));
		}
	
	}
	private Text countryidentifier(Text key) {
		if(key.toString().contains("IN")) {
			country="IN";
			String keystr=key.toString().replace("IN", "");
			key = new Text(keystr);
		}
		if(key.toString().contains("US")) {
			country="US";
			String keystr=key.toString().replace("US", "");
			key = new Text(keystr);
		}
		if(key.toString().contains("CA")) {
			country="CA";
			String keystr=key.toString().replace("CA", "");
			key = new Text(keystr);
		}
		if(key.toString().contains("FR")) {
			country="FR";
			String keystr=key.toString().replace("FR", "");
			key = new Text(keystr);
		}
		if(key.toString().contains("RU")) {
			country="RU";
			String keystr=key.toString().replace("RU", "");
			key = new Text(keystr);
		}
		return key;
	}
	}

