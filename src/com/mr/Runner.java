package com.mr;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class Runner {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		YoutubeReducer yr = new YoutubeReducer();
		JobConf conf = new JobConf(Runner.class);
		conf.setJobName("YoutubeData");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(YoutubeMapper.class);
		conf.setReducerClass(YoutubeReducer.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		firstjob(conf,args);
		yr.AnalysingCache();
		secondjob(conf,args);
		thirdjob(conf,args);
		fourthjob(conf,args);
		fifthjob(conf,args);
	}

	private static void fifthjob(JobConf conf, String[] args) throws IOException {
		FileInputFormat.setInputPaths(conf, new Path(args[4]));
		FileOutputFormat.setOutputPath(conf, new Path(args[9]));
		JobClient.runJob(conf);	
	}

	private static void fourthjob(JobConf conf, String[] args) throws IOException {
		FileInputFormat.setInputPaths(conf, new Path(args[3]));
		FileOutputFormat.setOutputPath(conf, new Path(args[8]));
		JobClient.runJob(conf);	
	}

	private static void thirdjob(JobConf conf, String[] args) throws IOException {
		FileInputFormat.setInputPaths(conf, new Path(args[2]));
		FileOutputFormat.setOutputPath(conf, new Path(args[7]));
		JobClient.runJob(conf);	
	}

	private static void secondjob(JobConf conf, String[] args) throws IOException {
		FileInputFormat.setInputPaths(conf, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf, new Path(args[6]));
		JobClient.runJob(conf);		
	}

	private static void firstjob(JobConf conf, String[] args) throws IOException {
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[5]));
		JobClient.runJob(conf);
	}

}
