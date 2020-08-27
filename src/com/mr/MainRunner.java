package com.mr;

import java.io.IOException;

public class MainRunner {

	public static void main(String[] args) throws IOException {
		PostgresqlJdbcConnection pg = new PostgresqlJdbcConnection();
		pg.deleteAllData();
		Top10Runner top10 = new Top10Runner();
		ViewsRunner viewsrun = new ViewsRunner();
		InteractiveRunner interactiverun = new InteractiveRunner();
		try {
			top10.top10tasks(args);
			viewsrun.viewtasks(args);
			interactiverun.interactivetasks(args);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
}
}