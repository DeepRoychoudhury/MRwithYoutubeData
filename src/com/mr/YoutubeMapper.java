package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class YoutubeMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	private String KEYSPACE_NAME="test";
	private Cluster cluster;
	public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
	{
		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		Session session = cluster.connect(KEYSPACE_NAME);
		
		String createtablequery = "create ColumnFamily IF NOT EXISTS youtubeindia (video_id varchar PRIMARY KEY,trending_date timestamp,title varchar,channel_title varchar,category_id int,publish_time timestamp,tags varchar,views bigint,likes bigint,dislikes bigint,comment_count bigint,thumbnail_link varchar,comments_disabled Boolean,ratings_disabled Boolean,video_error_or_removed Boolean,description varchar);"; 
		session.execute(createtablequery);
		
		  String line = value.toString(); 
		  String[] words=line.split(","); 
		  for(String word: words ) { 
		  Text outputKey = new Text(word.toUpperCase().trim());
		  IntWritable outputValue = new IntWritable(1); 
		  con.write(outputKey,outputValue); 
		  }
		 
	}
}
