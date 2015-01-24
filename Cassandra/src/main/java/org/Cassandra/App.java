package org.Cassandra;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Hello world!
 * 
 */
public class App {
	public static void main(String[] args) {
		System.out.println("aaa");
		try {
			Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
			Connection con = DriverManager
					.getConnection("jdbc:cassandra://122.144.134.67:9042/demodb");
			String query2 = "select * from user_credit;";
			PreparedStatement statement = con.prepareStatement(query2);

			ResultSet executeQuery = statement.executeQuery();

//			String query = "INSERT INTO user_credit (user_id, right_credit) values ('000000000',10);";
//			 statement = con.prepareStatement(query);
//
//			statement.executeUpdate();

			statement.close();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
