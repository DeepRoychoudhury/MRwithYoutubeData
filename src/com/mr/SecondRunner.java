package com.mr;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class SecondRunner {
	 static String[] CONTACT_POINTS = {"127.0.0.1"};
	    static int PORT = 9042;
	    public static void main(String[] args) throws IOException {
	        SecondRunner client = new SecondRunner();
	            try {
	              client.connect(CONTACT_POINTS, PORT);
	              client.createSchema();
	              client.loadData();
	              client.querySchema();
	              
					/*
					 * Configuration c=new Configuration(); String[] files=new
					 * GenericOptionsParser(c,args).getRemainingArgs(); Path input=new
					 * Path(files[0]); Path output=new Path(files[1]); Job j=new
					 * Job(c,"youtubedata"); j.setJarByClass(Runner.class);
					 * j.setMapperClass(YoutubeMapper.class);
					 * j.setReducerClass(YoutubeReducer.class); j.setOutputKeyClass(Text.class);
					 * j.setOutputValueClass(IntWritable.class); FileInputFormat.addInputPath(j,
					 * input); FileOutputFormat.setOutputPath(j, output);
					 * System.exit(j.waitForCompletion(true)?0:1);
					 */
	      		
	            } finally {
	              client.close();
	            }
	          }
	        
	     private Cluster cluster;
	      private Session session;
	      
	      public void connect(String[] contactPoints, int port) {
	            cluster = Cluster.builder().addContactPoints(contactPoints).withPort(port).build();
	            System.out.printf("Connected to cluster: %s%n", cluster.getMetadata().getClusterName());
	            session = cluster.connect("test");
	          }
	      public void createSchema() {
	            session.execute(
	                "CREATE KEYSPACE IF NOT EXISTS test WITH replication "
	                    + "= {'class':'SimpleStrategy', 'replication_factor':1};");
	            session.execute(
	                "create table if not exists youtubeindia (" + 
	                "video_id varchar PRIMARY KEY," + 
	                "trending_date varchar," + 
	                "title varchar," + 
	                "channel_title varchar," + 
	                "category_id int," + 
	                "publish_time varchar," + 
	                "tags varchar," + 
	                "views bigint," + 
	                "likes bigint," + 
	                "dislikes bigint," + 
	                "comment_count bigint," + 
	                "thumbnail_link varchar," + 
	                "comments_disabled Boolean," + 
	                "ratings_disabled Boolean," + 
	                "video_error_or_removed Boolean," + 
	                "description varchar" + 
	                ");");
	                     }
	          /** Inserts data into the tables. */
	          public void loadData() {
					/*
					 * String csvFile = "/home/hduser/YoutubeDataset/US/USvideos.csv";
					 * BufferedReader br = null; String line = ""; String cvsSplitBy = ","; try { br
					 * = new BufferedReader(new FileReader(csvFile)); while ((line = br.readLine())
					 * != null) { String[] mydata = line.split(cvsSplitBy);
					 * if(!(mydata[0].contains("video_id"))) { System.out.println("////////");
					 * //String myquery
					 * ="INSERT INTO test.airlinesafe (airline_id,airline,avail_seat_km_per_week,incidents_85_99,fatal_accidents_85_99,fatalities_85_99,incidents_00_14,fatal_accidents_00_14,fatalities_00_14) VALUES("
					 * + mydata[0] + ", '" + mydata[1] + "'," + mydata[2] + "," + mydata[3] + ","+
					 * mydata[4] + "," + mydata[5] + ","+ mydata[6] + ","+ mydata[7] + "," +
					 * mydata[8] + ");"; String data =
					 * "'"+mydata[0]+"','"+mydata[1]+"','"+mydata[2]+"','"+mydata[3]+"',"+mydata[4]+
					 * ",'"+mydata[5]+"','"+mydata[6]+"',"+mydata[7]+","+mydata[8]+","+mydata[9]+","
					 * +mydata[10]+",'"+mydata[11]+"',"+mydata[12]+","+mydata[13]+","+mydata[14]+
					 * ",'"+mydata[15]+"'"; session.
					 * execute("INSERT INTO youtubeindia (video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,dislikes,comment_count,thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed,description) VALUES ('"
					 * +data+"')"); //session.
					 * execute("copy youtubeindia (video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,dislikes,comment_count,thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed,description) from '/home/hduser/YoutubeDataset/US/USvideos.csv' with HEADER = TRUE;"
					 * ); // System.out.println(myquery); } } } catch (FileNotFoundException e) {
					 * e.printStackTrace(); } catch (IOException e) { e.printStackTrace(); } finally
					 * { if (br != null) { try { br.close(); } catch (IOException e) {
					 * e.printStackTrace(); } }
					 */
	               // }
	                
	        /*
	         * session.execute(
	         * "INSERT INTO test.airlinesafe (airline_id,airline,avail_seat_km_per_week,incidents_85_99,fatal_accidents_85_99,fatalities_85_99,incidents_00_14,fatal_accidents_00_14,fatalities_00_14) "
	         * + "VALUES (" + "101," + "'a'," + "1," + "1," + "1," + "1," + "1," + "1," +
	         * "1" + ");");
	         */
	          }
	                     /** Queries and displays data. */
	          public void querySchema() {
	            ResultSet results =
	                session.execute(
	                    "SELECT * FROM youtubeindia;");
	            
				/*
				 * for (Row row : results) { System.out.printf(
				 * "%-30s\t%-20s\t%-20s%n%s%s%s%s%s%s", row.getInt("video_id"),
				 * row.getString("airline"), row.getLong("avail_seat_km_per_week"),
				 * row.getInt("incidents_85_99"), row.getInt("fatal_accidents_85_99"),
				 * row.getInt("fatalities_85_99"), row.getInt("incidents_00_14"),
				 * row.getInt("fatal_accidents_00_14"), row.getInt("fatalities_00_14")); }
				 */
	            System.out.println(results.all().toString());
	          }
	          /** Closes the session and the cluster. */
	          public void close() {
	            session.close();
	            cluster.close();
	          }
}
