package com.mr;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.io.Text;

public class PostgreDatabaseQueries {

	public void insertintotop10categories(String category_id, int uploads, String country, Connection con) {
		StringBuilder query = new StringBuilder();
		query=query.append("insert into top10categories(category_id,uploads,country) values ('").append(category_id).append("','").append(uploads).append("','").append(country).append("');");
		System.out.println(query);
		try {
			PreparedStatement ps = con.prepareStatement(query.toString());
			ps.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void deleteAllData(Connection con) {
		deletetop10categoriesmethod(con);
		deleteviewscount(con);		
	}

	public void deleteviewscount(Connection con) {
		StringBuilder deleteviews = new StringBuilder();
		deleteviews=deleteviews.append("delete from viewscount;");
		try {
			PreparedStatement psviewscount = con.prepareStatement(deleteviews.toString());
			psviewscount.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public void deletetop10categoriesmethod(Connection con) {
		StringBuilder deletetop10categories = new StringBuilder();
		deletetop10categories=deletetop10categories.append("delete from top10categories;");
		PreparedStatement pstop10;
		try {
			pstop10 = con.prepareStatement(deletetop10categories.toString());
			pstop10.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void deleteAllDatawithcountry(String country, Connection con) {
		StringBuilder deletetop10categories = new StringBuilder();
		deletetop10categories=deletetop10categories.append("delete from top10categories where country='").append(country).append("';");
		PreparedStatement ps;
		try {
			ps = con.prepareStatement(deletetop10categories.toString());
			ps.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void deleteAllViewsDatawithcountry(String country, Connection con) {
		StringBuilder deletetop10categories = new StringBuilder();
		deletetop10categories=deletetop10categories.append("delete from viewscount where country='").append(country).append("';");
		PreparedStatement ps;
		try {
			ps = con.prepareStatement(deletetop10categories.toString());
			ps.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void insertViewsData(Text key, double sum, double median, double sd, String country, Connection con) {
		StringBuilder query = new StringBuilder();
		query=query.append("insert into viewscount(category_id,views,country,standarddeviation,median) values ('").append(key).append("','").append(sum).append("','").append(country).append("','").append(median).append("','").append(sd).append("');");
		System.out.println(query);
		try {
			PreparedStatement ps = con.prepareStatement(query.toString());
			ps.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void insertInteractiveData(Text key, double sum, String country, double median, double sd, Connection con) {
		StringBuilder query = new StringBuilder();
		query=query.append("insert into interactioncount(category_id,interactioncount,country,standarddeviation,median) values ('").append(key).append("','").append(sum).append("','").append(country).append("','").append(sd).append("','").append(median).append("');");
		PreparedStatement ps;
		try {
			ps = con.prepareStatement(query.toString());
			ps.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void deleteinteractioncount(Connection con) {
		StringBuilder deleteinteraction = new StringBuilder();
		deleteinteraction=deleteinteraction.append("delete from interactioncount;");
		try {
			PreparedStatement psinteractioncount = con.prepareStatement(deleteinteraction.toString());
			psinteractioncount.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void updatestddevmedian(String str, String country, double median, double sd, Connection con) {
		StringBuilder deleterecord = new StringBuilder();
		StringBuilder insertrecord = new StringBuilder();
		deleterecord=deleterecord.append("delete from stddevmedian where country='").append(country).append("' AND operationtype='").append(str).append("';");
		insertrecord=insertrecord.append("insert into stddevmedian(operationtype,country,standarddeviation,median) values ('").append(str).append("','").append(country).append("','").append(sd).append("','").append(median).append("');");
		PreparedStatement psdeleterecord;
		PreparedStatement psinsertrecord;
		try {
			psdeleterecord = con.prepareStatement(deleterecord.toString());
			psdeleterecord.executeUpdate();
			psinsertrecord = con.prepareStatement(insertrecord.toString());
			psinsertrecord.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void deletestddevmedian(Connection con) {
		StringBuilder deleteinteraction = new StringBuilder();
		deleteinteraction=deleteinteraction.append("delete from stddevmedian;");
		try {
			PreparedStatement psinteractioncount = con.prepareStatement(deleteinteraction.toString());
			psinteractioncount.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


}
