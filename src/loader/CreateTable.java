package loader;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class CreateTable {

    private Connection connection;

    public CreateTable(final Connection connection) {
        this.connection = connection;
        createTables();
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(final Connection connection) {
        this.connection = connection;
    }

    public void createTables() {

        Statement statement = null;
        StringBuilder sqlSb = new StringBuilder();

        sqlSb.append(region())
                .append(nation())
                .append(customer())
                .append(supplier())
                .append(part())
                .append(partsupp())
                .append(orders())
                .append(lineitem());

        try {
            statement = this.connection.createStatement();
            statement.executeUpdate(sqlSb.toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public String region() {
        String sql;
        sql = "CREATE TABLE  region (\n" +
                "        r_regionkey INTEGER,\n" +
                "        r_name CHAR(25),\n" +
                "        r_comment VARCHAR(152)\n" +
                ");";
        return sql;
    }

    public String nation() {
        String sql;
        sql = "CREATE TABLE  nation (\n" +
                "        n_nationkey INTEGER,\n" +
                "        n_name CHAR(25),\n" +
                "        n_regionkey INTEGER,\n" +
                "        n_comment VARCHAR(152)\n" +
                ");";
        return sql;
    }

    public String customer() {
        String sql;
        sql = "CREATE TABLE  customer (\n" +
                "        c_custkey INTEGER,\n" +
                "        c_name VARCHAR(25),\n" +
                "        c_address VARCHAR(40),\n" +
                "        c_nationkey INTEGER,\n" +
                "        c_phone CHAR(15),\n" +
                "        c_acctbal NUMERIC,\n" +
                "        c_mktsegment CHAR(10),\n" +
                "        c_comment VARCHAR(117)   \n" +
                ");";
        return sql;
    }

    public String supplier() {
        String sql;
        sql = "CREATE TABLE  supplier (\n" +
                "        s_suppkey  INTEGER,\n" +
                "        s_name CHAR(25),\n" +
                "        s_address VARCHAR(40),\n" +
                "        s_nationkey INTEGER,\n" +
                "        s_phone CHAR(15),\n" +
                "        s_acctbal NUMERIC,\n" +
                "        s_comment VARCHAR(101)\n" +
                "\n" +
                ");";
        return sql;
    }

    public String part() {
        String sql;
        sql = "CREATE TABLE  part (\n" +
                "        p_partkey INTEGER,\n" +
                "        p_name VARCHAR(55),\n" +
                "        p_mfgr CHAR(25),\n" +
                "        p_brand CHAR(10),\n" +
                "        p_type VARCHAR(25),\n" +
                "        p_size INTEGER,\n" +
                "        p_container CHAR(10),\n" +
                "        p_retailprice NUMERIC,\n" +
                "        p_comment VARCHAR(23)\n" +
                ");";
        return sql;
    }

    public String partsupp() {
        String sql;
        sql = "CREATE TABLE  partsupp (\n" +
                "        ps_partkey INTEGER,\n" +
                "        ps_suppkey INTEGER,\n" +
                "        ps_availqty INTEGER,\n" +
                "        ps_supplycost NUMERIC,\n" +
                "        ps_comment VARCHAR(199)\n" +
                ");";
        return sql;
    }

    public String orders() {
        String sql;
        sql = "CREATE TABLE orders (\n" +
                "        o_orderkey BIGINT,\n" +
                "        o_custkey INTEGER,\n" +
                "        o_orderstatus CHAR(1),\n" +
                "        o_totalprice NUMERIC,\n" +
                "        o_orderdate DATE,\n" +
                "        o_orderpriority CHAR(15),\n" +
                "        o_clerk CHAR(15),\n" +
                "        o_shippriority INTEGER,\n" +
                "        o_comment VARCHAR(79)\n" +
                ");";
        return sql;
    }

    public String lineitem() {
        String sql;
        sql = "CREATE TABLE  lineitem (\n" +
                "        l_orderkey BIGINT,\n" +
                "        l_partkey INTEGER,\n" +
                "        l_suppkey INTEGER,\n" +
                "        l_linenumber INTEGER,\n" +
                "        l_quantity NUMERIC,\n" +
                "        l_extendedprice NUMERIC,\n" +
                "        l_discount NUMERIC,\n" +
                "        l_tax NUMERIC,\n" +
                "        l_returnflag CHAR(1),\n" +
                "        l_linestatus CHAR(1),\n" +
                "        l_shipdate DATE,\n" +
                "        l_commitdate DATE,\n" +
                "        l_receiptdate DATE,\n" +
                "        l_shipinstruct CHAR(25),\n" +
                "        l_shipmode CHAR(10),\n" +
                "        l_comment VARCHAR(44)\n" +
                ");";
        return sql;
    }
}
