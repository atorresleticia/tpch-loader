package loader.postgresql;

import nl.cwi.monetdb.mcl.MCLException;
import nl.cwi.monetdb.mcl.io.BufferedMCLReader;
import nl.cwi.monetdb.mcl.io.BufferedMCLWriter;
import nl.cwi.monetdb.mcl.net.MapiSocket;
import nl.cwi.monetdb.mcl.parser.MCLParseException;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;

public class PostgresCopy {

    private Connection connection;
    private Integer sf;
    private String basePath;

    public PostgresCopy(Connection connection, String basePath, Integer sf) {
        this.connection = connection;
        this.basePath = basePath;
        this.sf = sf;
        copyMonet();
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public void cp(String relation, CopyManager copyManager) {

        String sql = String.format("COPY INTO %s FROM stdin USING DELIMITERS '|', '\n'", relation.toLowerCase());
        Reader in = null;
        try {
            in = (relation.equals("region") || relation.equals("nation")) ?
                    new BufferedReader(new FileReader(new File(String.format("%s\\\\%s.tbl", this.basePath, relation))))
                    : new BufferedReader(new FileReader(new File(String.format("%s\\\\%s_%d.tbl", this.basePath, relation, this.sf))));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        long rowsAffected = 0;
        try {
            rowsAffected = copyManager.copyIn(sql, in);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(rowsAffected);
    }

    public void cpMonet(String relation){

        MapiSocket server = new MapiSocket();
        server.setDatabase("demo");
        server.setLanguage("sql");
        try {
            server.connect("localhost", 50000, "monetdb", "monetdb");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (MCLParseException e) {
            e.printStackTrace();
        } catch (MCLException e) {
            e.printStackTrace();
        }

        String sql = String.format("COPY INTO %s FROM stdin USING DELIMITERS '|', '\n'", relation.toLowerCase());

        BufferedMCLReader in = server.getReader();
        BufferedMCLWriter out = server.getWriter();

        try {
            out.write('s');
            out.write(sql);
            out.newLine();
            out.writeLine("");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void copyDataToDatabase() throws SQLException {

        BaseConnection baseConnection = (BaseConnection) this.connection;
        CopyManager copyManager = new CopyManager(baseConnection);

        cp("region", copyManager);
        cp("nation", copyManager);
        cp("supplier", copyManager);
        cp("part", copyManager);
        cp("partsupp", copyManager);
        cp("customer", copyManager);
        cp("orders", copyManager);
        cp("lineitem", copyManager);
    }

    public void copyMonet() {
        cpMonet("region");
    }
}
