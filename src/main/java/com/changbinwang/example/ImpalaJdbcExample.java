package com.changbinwang.example;

import com.google.common.base.Stopwatch;
import org.apache.commons.codec.digest.DigestUtils;
import redis.clients.jedis.Jedis;

<<<<<<< HEAD:src/main/java/com/changbinwang/example/ImpalaJdbcExample.java
public class ImpalaJdbcExample {
	
	// 需要运行的query
	private static final String SQL_STATEMENT1 = "select compid,sum(case isimp when 0 then usd else 0 end) usd_exp,sum(case isimp when 1 then usd else 0 end) usd_imp from collect_2006 group by compid";
    private static final String SQL_STATEMENT2 = "select hs,compid,sum(case isimp when 0 then num else 0 end) num_exp,sum(case isimp when 1 then num else 0 end) num_imp from  collect_2006 group by hs,compid";
	
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
=======
import java.sql.*;
import java.util.List;
>>>>>>> concurrent:src/main/java/com/changbinwang/example/ClouderaImpalaJdbcExample.java

public class ClouderaImpalaJdbcExample {




    private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
	public static void main(String[] args) {

        String SQL_STATEMENT1 = args[0];

        // impalad的启动地址
        String IMPALAD_HOST1 = args[1];
        String IMPALAD_HOST2 = args[2];
        String IMPALAD_HOST3 = args[3];

        int concurrentNum = Integer.parseInt(args[4]);

        // port 21050 is the default impalad JDBC port
        String IMPALAD_JDBC_PORT = "21050";

        String CONNECTION_URL1 = "jdbc:hive2://" + IMPALAD_HOST1 + ':' + IMPALAD_JDBC_PORT + "/;auth=noSasl";
        String CONNECTION_URL2 = "jdbc:hive2://" + IMPALAD_HOST2 + ':' + IMPALAD_JDBC_PORT + "/;auth=noSasl";
        String CONNECTION_URL3 = "jdbc:hive2://" + IMPALAD_HOST3 + ':' + IMPALAD_JDBC_PORT + "/;auth=noSasl";




		System.out.println("\n=============================================");
		System.out.println("Cloudera Impala JDBC Example");
		System.out.println("Using Connection URL: " + CONNECTION_URL1);
        System.out.println("Using Connection URL: " + CONNECTION_URL2);
        System.out.println("Using Connection URL: " + CONNECTION_URL3);
        System.out.println("The sql statement is "+ SQL_STATEMENT1);


        for (int i = 0; i < concurrentNum; i++) {
           if (i%3==0){
               Thread thread = new Thread(new RunQuery(SQL_STATEMENT1,CONNECTION_URL1));
               thread.start();
           }else if(i%3==1){
               Thread thread = new Thread(new RunQuery(SQL_STATEMENT1,CONNECTION_URL2));
               thread.start();
           }else{
               Thread thread = new Thread(new RunQuery(SQL_STATEMENT1,CONNECTION_URL3));
               thread.start();
           }


            
        }
    }

    private static class RunQuery implements  Runnable{

        Jedis jedis = new Jedis("localhost");

        private String SQL_STATEMENT;
        private String CONNECTION_URL;
        private Connection con = null;
        private Statement stmt = null;
        private ResultSet rs = null;

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
                stmt = con.createStatement();
                System.out.println("Running Query: " + SQL_STATEMENT);

                if(jedis.exists("length" + DigestUtils.md5Hex(SQL_STATEMENT))){
                    List<String> result = jedis.lrange(DigestUtils.md5Hex(SQL_STATEMENT),0,Integer.parseInt(jedis.get("length"+DigestUtils.md5Hex(SQL_STATEMENT))));
                }else{
                    rs = stmt.executeQuery(SQL_STATEMENT);

                    Long length=0L;
                    // print the results to the console
                    while (rs != null && rs.next()) {
                        length = jedis.lpush(DigestUtils.md5Hex(SQL_STATEMENT), new String[]{rs.toString()});
                    }
                    if(length!=0){
                        jedis.set("length"+DigestUtils.md5Hex(SQL_STATEMENT),length.toString());
                    }
                }
                timer.stop();
                System.out.println("The time used for excution: "+timer.elapsedMillis());

            } catch (SQLException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    rs.close();
                    stmt.close();
                    con.close();
                } catch (Exception e) {
                    // swallow
                }
            }


        }
    }
}
