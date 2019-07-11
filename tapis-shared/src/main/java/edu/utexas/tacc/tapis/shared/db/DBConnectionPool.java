package edu.utexas.tacc.tapis.shared.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import edu.utexas.tacc.tapis.shared.parameters.Settings;


import java.sql.Connection;
import java.sql.SQLException;

public class DBConnectionPool {

    private static HikariConfig config = new HikariConfig();
    private static HikariDataSource ds;
    private static String DB_USERNAME = Settings.get("TAPIS_DB_USERNAME");
    private static String DB_PASSWORD = Settings.get("TAPIS_DB_PASSWORD");
    private static String DB_URL = Settings.get("TAPIS_DB_URL");

    static {
        config.setJdbcUrl(DB_URL);
        config.setUsername(DB_USERNAME);
        config.setPassword(DB_PASSWORD);
        config.setDriverClassName("org.postgresql.ds.PGSimpleDataSource");
        ds = new HikariDataSource(config);
    }

    public static Connection getConnection() throws SQLException {
        return ds.getConnection();
    }

    private DBConnectionPool(){}
}
