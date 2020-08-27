package com.mr;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PostgresqlJdbcConnection {

	public void addData(String country, String sportsplayed) {
		String url = "jdbc:postgresql://sportsdatabase.cpwytlyekgvv.us-east-1.rds.amazonaws.com:5432/sports";
        String user = "postgres";
        String password = "postgres";
        
        try {
			Class.forName("org.postgresql.Driver");
	    	Connection con = DriverManager.getConnection(url, user, password);        
	        //SportsDao sportsdao = new SportsDao();
	        //sportsdao.addData(country,sportsplayed,con);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
	}
}
