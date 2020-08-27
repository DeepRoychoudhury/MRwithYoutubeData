package com.mr;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class ViewsRunner {

	public void viewtasks(String[] args) throws IOException {
		JobConf conf = new JobConf(ViewsRunner.class);
		conf.setJobName("mystddevjob");
		conf.setMapperClass(ViewsYoutubeMapper.class);
		conf.setReducerClass(ViewsYoutubeReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);	
		firstcountry(conf,args);
		secondcountry(conf,args);
		thirdcountry(conf,args);
		fourthcountry(conf,args);
		fifthcountry(conf,args);
	}
	
	public static void firstcountry(JobConf conf,String[] args) throws IOException {
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[5]+"/IN/viewsdata"));
		JobClient.runJob(conf);
	}
	public static void secondcountry(JobConf conf,String[] args) throws IOException {
		FileInputFormat.setInputPaths(conf, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf, new Path(args[6]+"/US/viewsdata"));
		JobClient.runJob(conf);
	}
	public static void thirdcountry(JobConf conf,String[] args) throws IOException {
		FileInputFormat.setInputPaths(conf, new Path(args[2]));
		FileOutputFormat.setOutputPath(conf, new Path(args[7]+"/CA/viewsdata"));
		JobClient.runJob(conf);
	}
	public static void fourthcountry(JobConf conf,String[] args) throws IOException {
		FileInputFormat.setInputPaths(conf, new Path(args[3]));
		FileOutputFormat.setOutputPath(conf, new Path(args[8]+"/FR/viewsdata"));
		JobClient.runJob(conf);
	}
	public static void fifthcountry(JobConf conf,String[] args) throws IOException {
		FileInputFormat.setInputPaths(conf, new Path(args[4]));
		FileOutputFormat.setOutputPath(conf, new Path(args[9]+"/RU/viewsdata"));
		JobClient.runJob(conf);
	}
}
