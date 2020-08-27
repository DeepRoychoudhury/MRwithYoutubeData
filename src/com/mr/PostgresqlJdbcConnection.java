package com.mr;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.io.Text;

public class PostgresqlJdbcConnection {
	Connection con;
	PostgreDatabaseQueries pgquery = new PostgreDatabaseQueries();
	public void connect() {
		String url = "jdbc:postgresql://pda.cpwytlyekgvv.us-east-1.rds.amazonaws.com:5432/YoutubeMR";
        String user = "postgres";
        String password = "postgres";
        
        try {
			Class.forName("org.postgresql.Driver");
	    	con = DriverManager.getConnection(url, user, password);
	    	System.out.println("Database Connected Successfully");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void inserttop10Data(String category_id,int uploads,String country) {
        connect();
        pgquery.insertintotop10categories(category_id,uploads,country,con);
	}

	public void deleteAllData() {
		deletetop10categoriesmethod();
		deleteviewscount();
		deleteinteractioncount();
		deletestddevmedian();
	}

	private void deletestddevmedian() {
		connect();
		pgquery.deletestddevmedian(con);
	}

	private void deleteinteractioncount() {
		connect();
		pgquery.deleteinteractioncount(con);
	}

	private void deleteviewscount() {
		connect();
		pgquery.deleteviewscount(con);
	}

	private void deletetop10categoriesmethod() {
		connect();
		pgquery.deletetop10categoriesmethod(con);
	}

	public void deleteAllDatawithcountry(String country) {
		connect();
		pgquery.deleteAllDatawithcountry(country,con);
	}

	public void deleteAllViewsDatawithcountry(String country) {
		connect();
		pgquery.deleteAllViewsDatawithcountry(country,con);
	}

	public void insertViewsData(Text key, double sum, double median, double sd, String country) {
		connect();
		pgquery.insertViewsData(key,sum,median,sd,country,con);
		
	}

	public void insertInteractiveData(Text key, double sum, String country, double median, double sd) {
		connect();
		pgquery.insertInteractiveData(key,sum,country,median,sd,con);
	}

	public void updatestddevmedian(String str, String country, double median, double sd) {
		connect();
		pgquery.updatestddevmedian(str,country,median,sd,con);
	}
}
