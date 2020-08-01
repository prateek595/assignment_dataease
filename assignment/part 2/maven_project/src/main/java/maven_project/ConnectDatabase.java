package maven_project;


import java.sql.Connection;
import java.sql.DriverManager;

public class ConnectDatabase {
	public static Connection getConnection() throws Exception
	{
		 Connection con = null;
	      
	         //Registering the HSQLDB JDBC driver
	         Class.forName("org.hsqldb.jdbc.JDBCDriver");
	         //Creating the connection with HSQLDB
	         con = DriverManager.getConnection("jdbc:hsqldb:hsql://localhost/testdb", "SA", "");
	         return con;
	       }
}