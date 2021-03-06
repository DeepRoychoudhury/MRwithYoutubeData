package com.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class InteractiveYoutubeReducer extends MapReduceBase implements Reducer<Text, Text, Text, MedianStdDevTuple>{
	public List<Double> list = new ArrayList<Double>();
	public MedianStdDevTuple objStdDev = new MedianStdDevTuple();
	ArrayList<Double> med = new ArrayList<Double>();
	double median;
	double stddev;
	int count=0;
	String country;
	PostgresqlJdbcConnection pg = new PostgresqlJdbcConnection();
	
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, MedianStdDevTuple> output,
			Reporter reporter) throws IOException {
		System.out.println(key);
		
		//country identifier
		key = countryidentifier(key);
		System.out.println(country);
		
		double sum=0.0d;
		list.clear();
		 while (values.hasNext()) {  
			String number=values.next().toString();
			System.out.println("Number : "+number);
			if (number.contains("likes") && !number.contains("dis") && !number.contains(":likes")){
				String splittednum=number.replace("likes:", "");
			double num = Double.parseDouble(splittednum); 
			sum+=num;    
			list.add(num);
			}
			else if(number.contains("dislikes") && !number.contains(":dislikes")) {
				String splittednum=number.replace("dislikes:", "");
				double num = Double.parseDouble(splittednum); 
				sum+=num; 
				list.add(num);
			}
			else if(number.contains("comment_count") && !number.contains(":comment_count")) {
				String splittednum=number.replace("comment_count:", "");
				double num = Double.parseDouble(splittednum); 
				sum+=num; 			
				list.add(num);
			}
		}		


			count++;
			if(sum!=0.0) {
			med.add(sum);
			Collections.sort(med);
			}
		//cache.put(key, sum);
		
		
		//median calculation
				if(count>1) {
					System.out.println(count);
				if(count % 2 == 0 && count > 2) {
					System.out.println(med.get((count/2)-2) + " : " +med.get((int) (((count/2)-1))));
					if(((med.get((count/2)-2)+med.get((count/2)-1))/2.0)>=0) {
					double alg = (double) ((med.get((count/2)-2)+med.get((count/2)-1))/2.0);
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
				objStdDev.setMedian(Math.round(median));
				
				//finding standard deviation
				double mean = sum / count;
				double sumOfSquares = 0;
				for (double doubleWritable : list) {
				sumOfSquares += (doubleWritable - mean) * (doubleWritable - mean);
				}
				stddev = (double) Math.sqrt(sumOfSquares / (count - 1));
				objStdDev.setSd(Math.round(stddev));
				System.out.println("Standard Deviation is : "+stddev);
				
				pg.insertInteractiveData(key,sum,country,objStdDev.getMedian(),objStdDev.getSd());
				pg.updatestddevmedian("interactive",country,objStdDev.getMedian(),objStdDev.getSd());
		output.collect(key, objStdDev);
	
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
