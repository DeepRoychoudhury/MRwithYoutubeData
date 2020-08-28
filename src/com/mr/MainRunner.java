package com.mr;

import java.io.IOException;

public class MainRunner {
	static String[] CONTACT_POINTS = { "127.0.0.1" };
	static int PORT = 9042;

	public static void main(String[] args) throws IOException {
		
		CassandraConnection cascon = new CassandraConnection();
		cascon.connect(CONTACT_POINTS, PORT);
		cascon.createSchema();
		cascon.loadData();
		/*
		 * cascon.connectToCassandra(); cascon.insertToCassandra();
		 */
		/*
		 * PostgresqlJdbcConnection pg = new PostgresqlJdbcConnection();
		 * pg.deleteAllData(); Top10Runner top10 = new Top10Runner(); ViewsRunner
		 * viewsrun = new ViewsRunner(); InteractiveRunner interactiverun = new
		 * InteractiveRunner(); try { top10.top10tasks(args); viewsrun.viewtasks(args);
		 * interactiverun.interactivetasks(args); } catch (ClassNotFoundException e) {
		 * // TODO Auto-generated catch block e.printStackTrace(); } catch (IOException
		 * e) { // TODO Auto-generated catch block e.printStackTrace(); } catch
		 * (InterruptedException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); }
		 */
}
}