package com.changbinwang.example;

import com.google.common.base.Stopwatch;
import org.apache.commons.lang.time.StopWatch;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ClouderaImpalaJdbcExample {
	
	// 需要运行的query
    private static final String SQL_STATEMENT2 = "select hs,compid,sum(case isimp when 0 then num else 0 end) num_exp,sum(case isimp when 1 then num else 0 end) num_imp from  collect_2006 group by hs,compid";
    private static final String SQL_STATEMENT1 = "select compid,sum(case isimp when 0 then usd else 0 end) usd_exp,sum(case isimp when 1 then usd else 0 end) usd_imp from collect_2006 group by compid";

	// impalad的启动地址
	private static final String IMPALAD_HOST = "172.16.189.130";

	// port 21050 is the default impalad JDBC port
	private static final String IMPALAD_JDBC_PORT = "21050";

	private static final String CONNECTION_URL = "jdbc:hive2://" + IMPALAD_HOST + ':' + IMPALAD_JDBC_PORT + "/;auth=noSasl";

	private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";


	public static void main(String[] args) {

		System.out.println("\n=============================================");
		System.out.println("Cloudera Impala JDBC Example");
		System.out.println("Using Connection URL: " + CONNECTION_URL);


        for (int i = 0; i < 30; i++) {
           Thread thread = new Thread(new RunQuery(SQL_STATEMENT1,CONNECTION_URL));
            thread.start();

            
        }
    }

    private static class RunQuery implements  Runnable{

        private String SQL_STATEMENT;
        private String CONNECTION_URL;
        private Connection con = null;

        RunQuery(String SQL_STATEMENT, String CONNECTION_URL ) {
            this.SQL_STATEMENT = SQL_STATEMENT;
            this.CONNECTION_URL = CONNECTION_URL;
        }
        private Stopwatch timer = new Stopwatch();

        @Override
        public void run() {

            try {
                Class.forName(JDBC_DRIVER_NAME);
                timer.start();
                con = DriverManager.getConnection(CONNECTION_URL);
                Statement stmt = con.createStatement();
//                System.out.println("Running Query: " + SQL_STATEMENT);

                ResultSet rs = stmt.executeQuery(SQL_STATEMENT);
                timer.stop();
                System.out.println("The time used for excution: "+timer.elapsedMillis());

//                System.out.println("\n== Begin Query Results ======================");

                // print the results to the console
                while (rs.next()) {
                    // the example query returns one String column
//                    System.out.println(rs.getString(1));
                }

//                System.out.println("== End Query Results =======================\n\n");
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    con.close();
                } catch (Exception e) {
                    // swallow
                }
            }


        }
    }
}
