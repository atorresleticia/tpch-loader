package connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author Leticia Torres <atorresleticia@gmail.com>
 * @since 2018-04-11
 */
public class ConnectionFactory {

    private final String host;
    private final String url;
    private final String user;
    private final String password;
    private final String sgbd;
    private final String database;

    public ConnectionFactory(String host, String user, String password, String sgbd, String database) {
        this.sgbd = sgbd;
        this.database = database;
        this.host = host;
        this.url = String.format("jdbc:%s://%s/%s", sgbd, host, database);
        this.user = user;
        this.password = password;
    }

    public Connection getConnection() {

        Connection connection = null;
        try {
            connection = DriverManager.getConnection(this.url, this.user, this.password);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return connection;
    }
}
