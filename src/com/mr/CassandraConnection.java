package com.mr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

public class CassandraConnection {
	
	private static String KEYSPACE_NAME="test";
	static String[] CONTACT_POINTS = { "127.0.0.1" };
	static int PORT = 9042;
	private Cluster cluster;
	private Session session;

	public void connectToCassandra() {
	String query = "select * from youtubeindia;";	
	Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
	Session session = cluster.connect(KEYSPACE_NAME);
	ResultSet rs = session.execute(query);
	System.out.println(rs.getExecutionInfo());
	System.out.println(rs.all());
	
	}
	
	
	public void insertToCassandra() {
		String query = "select * from youtubeindia;";	
		Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		Session session = cluster.connect(KEYSPACE_NAME);
		ResultSet rs = session.execute(query);
		System.out.println(rs.getExecutionInfo());
		System.out.println(rs.all());
		
		}

	public void connect(String[] CONTACT_POINTS2, int port) {
		cluster = Cluster.builder().addContactPoints(CONTACT_POINTS2).withPort(port).build();
		System.out.printf("Connected to cluster: %s%n", cluster.getMetadata().getClusterName());
		session = cluster.connect();
	}
	
	public void loadData() {
		String csvFile = "/home/hduser/YoutubeDataset/IN/INvideos.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";
		try {
			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {
				String[] mydata = line.split(cvsSplitBy);
				if (!(mydata[0].contains("video_id"))) {
					session.execute(
							"INSERT INTO test.youtubeindia (video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,dislikes,comment_count,thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed,description" + 
							") "
									+ "VALUES (" + mydata[0] + "," + "'" + mydata[1] + "'," + "" + mydata[2] + "," + ""
									+ mydata[3] + "," + "" + mydata[4] + "," + "" + mydata[5] + "," + "" + mydata[6]
									+ "," + "" + mydata[7] + "," + "" + mydata[8] + "" + "" + mydata[9] + "," + "" + 
									mydata[10] + "," + "" + mydata[11] + "," + "" + mydata[12] + "," + "" + mydata[13] + "," + "" + mydata[14] + "," + "" + mydata[15] + "" + ");");
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public void createSchema() {
		session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH replication "
				+ "= {'class':'SimpleStrategy', 'replication_factor':1};");
		session.execute("CREATE TABLE IF NOT EXISTS test.youtubeindia (" + "video_id uuid PRIMARY KEY,"
				+ "trending_date text," + "title text," + "channel_title text,"
				+ "category_id int," + "publish_time text," + "tags text,"
				+ "views double," + "likes double" + "dislikes double" + "comment_count double" + "thumbnail_link text" + "comments_disabled boolean" + "ratings_disabled boolean" + "video_error_or_removed boolean" + "description text" + ");");
	}
		
	
}