package com.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Writable;

public class Top10MedianStdDevTuple implements Writable{
	
	private HashMap<String,Integer> top10categories = new HashMap<String,Integer>();
	private float median = 0f;
	private float stddev = 0f;

	@Override
	public void readFields(DataInput input) throws IOException {		
		median = input.readFloat();
		stddev = input.readFloat();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeFloat(median);
		output.writeFloat(stddev);
	}

	public HashMap<String,Integer> getTop10categories() {
		return top10categories;
	}

	public void setTop10categories(HashMap<String,Integer> top10categories) {
		this.top10categories = top10categories;
	}

	public float getMedian() {
		return median;
	}

	public void setMedian(float median) {
		this.median = median;
	}

	public float getStddev() {
		return stddev;
	}

	public void setStddev(float stddev) {
		this.stddev = stddev;
	}

	@Override
	public String toString() {
		return "Top10MedianStdDevTuple [top10categories=" + top10categories + ", median=" + median + ", stddev="
				+ stddev + "]";
	}

	
}
