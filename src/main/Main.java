package main;

import connection.ConnectionFactory;
import loader.CreateTable;
import loader.postgresql.PostgresCopy;

import java.sql.Connection;
import java.sql.SQLException;

public class Main {
    public static void main(String[] args) throws SQLException {

        ConnectionFactory connectionFactory = new ConnectionFactory("localhost", "monetdb", "monetdb", "monetdb", "demo");
        Connection connection = connectionFactory.getConnection();
        String basePath = "D:\\\\TPCH\\\\DATA\\\\ORIGINAL";

        //new CreateTable(connection);
        new PostgresCopy(connection, basePath, 1);

        connection.close();
    }
}
