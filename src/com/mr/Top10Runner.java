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


public class Top10Runner {

	public void top10tasks(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		JobConf conf = new JobConf(Top10Runner.class);
		conf.setJobName("YoutubeData");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Top10YoutubeMapper.class);
		conf.setReducerClass(Top10YoutubeReducer.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		firstjob(conf,args);
		secondjob(conf,args);
		thirdjob(conf,args);
		fourthjob(conf,args);
		fifthjob(conf,args);
	}

	private static void fifthjob(JobConf conf, String[] args) throws IOException {
		FileInputFormat.setInputPaths(conf, new Path(args[4]));
		FileOutputFormat.setOutputPath(conf, new Path(args[9]+"/RU/top10"));
		JobClient.runJob(conf);	
	}

	private static void fourthjob(JobConf conf, String[] args) throws IOException {
		FileInputFormat.setInputPaths(conf, new Path(args[3]));
		FileOutputFormat.setOutputPath(conf, new Path(args[8]+"/FR/top10"));
		JobClient.runJob(conf);	
	}

	private static void thirdjob(JobConf conf, String[] args) throws IOException {
		FileInputFormat.setInputPaths(conf, new Path(args[2]));
		FileOutputFormat.setOutputPath(conf, new Path(args[7]+"/CA/top10"));
		JobClient.runJob(conf);	
	}

	private static void secondjob(JobConf conf, String[] args) throws IOException {
		FileInputFormat.setInputPaths(conf, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf, new Path(args[6]+"/US/top10"));
		JobClient.runJob(conf);		
	}

	private static void firstjob(JobConf conf, String[] args) throws IOException {
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[5]+"/IN/top10"));
		JobClient.runJob(conf);
	}

}
