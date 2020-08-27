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
public class ViewsYoutubeReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, MedianStdDevTuple> {
	public List<Double> list = new ArrayList<Double>();
	public MedianStdDevTuple objStdDev = new MedianStdDevTuple();
	ArrayList<Double> med = new ArrayList<Double>();
	double median = 0;
	double stddev = 0;
	int count=0;
	String country;

	PostgresqlJdbcConnection pg = new PostgresqlJdbcConnection();
	
	@Override
	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, MedianStdDevTuple> output,
			Reporter reporter) throws IOException {
		System.out.println(key);
		
		//country identifier
		key = countryidentifier(key);
		System.out.println(country);
			double sum = 0;
		list.clear();
		 while (values.hasNext()) {  
			double number=Double.parseDouble(values.next().toString());
			System.out.println("Number : "+number);
			double num = number; 
			sum+=num;
			list.add(num);	
		}		 

			med.add(sum);
			count++;
		System.out.println("Key : "+key + " Sum of views : "+sum);
		
		//sorting in ascending order
		Collections.sort(med);
		
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
		System.out.println("Median value is : "+median);
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
		//pg.deleteAllViewsDatawithcountry(country);
		pg.insertViewsData(key,sum,objStdDev.getMedian(),objStdDev.getSd(),country);
		pg.updatestddevmedian("views",country,objStdDev.getMedian(),objStdDev.getSd());
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