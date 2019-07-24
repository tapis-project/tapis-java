package edu.utexas.tacc.tapis.shareddb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.zaxxer.hikari.HikariDataSource;

import edu.utexas.tacc.tapis.shareddb.datasource.HikariDSGenerator;
import edu.utexas.tacc.tapis.shareddb.datasource.HikariDSGenerator.MetricReporterType;

@Test(groups={"integration"})
public final class TempDBTest 
{
    /* **************************************************************************** */
    /*                                  Constants                                   */
    /* **************************************************************************** */
    // Hikari configuration parameters.
    private static final String  TEST_DB  = "testdb";
    private static final String  ADMIN_DB = "postgres";
    private static final String  BASE_URL = "jdbc:postgresql://localhost:5432/";
    private static final String  USER = "tapis";
    private static final String  PASSWORD = "password";
    private static final int     MAX_POOL_SIZE = 2;
    
    // Temporary table created by this program.
    private static final String  ADMIN_POOL = "adminPool";
    private static final String  TEST_POOL  = "testPool";
    private static final String  TABLE_NAME = "testTable";
    
    /* **************************************************************************** */
    /*                                   Fields                                     */
    /* **************************************************************************** */
    // Set to enable the database to be dropped by disconnecting all connections.
    private HikariDataSource _testDataSource;
    
    /* **************************************************************************** */
    /*                                    Tests                                     */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* testDS:                                                                      */
    /* ---------------------------------------------------------------------------- */
    @Test(enabled=true)
    public void allTest() throws SQLException
    {
        // ----------------- DB Management -----------------
        // Connect to the admin database.
        Connection adminConn = connectDB(ADMIN_DB, ADMIN_POOL);
        
        // Drop the test database if it exists.
        dropDB(adminConn);
        
        // Create the test database.
        createDB(adminConn);
        
        // ---------------- Data Management ----------------
        // Connect to the just created testdb.
        Connection testConn = connectDB(TEST_DB, TEST_POOL);
        
        // Create a table.
        createTable(testConn);
        
        // Write the table.
        InsertIntoTable(testConn);
        
        // Read the table.
        readTable(testConn);
        
        // Drop the table.
        dropTable(testConn);
        
        // Close the data connection.
        testConn.close();
        
        // Close the data source to remove all cached connections.
        _testDataSource.close();
        
        // ----------------- DB Management -----------------
        // Drop the database and close the admin connection.
        dropDB(adminConn);
        adminConn.close();
    }

    /* **************************************************************************** */
    /*                                Private Methods                               */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* connectDB:                                                                   */
    /* ---------------------------------------------------------------------------- */
    private Connection connectDB(String dbName, String poolName)
    {
        // Get an Hikari data source using the no-args constructor.
        HikariDSGenerator dsgen = new HikariDSGenerator();
        HikariDataSource ds = dsgen.getDataSource();
        ds.setPoolName(poolName);
        ds.setMaximumPoolSize(MAX_POOL_SIZE);
        ds.setJdbcUrl(BASE_URL + dbName);
        ds.addDataSourceProperty("user", USER);
        ds.addDataSourceProperty("password", PASSWORD);
        ds.addDataSourceProperty("ApplicationName", getClass().getSimpleName());
        ds.addDataSourceProperty("assumeMinServerVersion", "11.4");

        // Customize connection to spit out metrics once a minute.
        dsgen.setMetricRegistry(ds, 1, MetricReporterType.SL4J);
        
        // Get the connection.
        Connection connection = null;
        try {connection = ds.getConnection();}
        catch (SQLException e) {
            e.printStackTrace();
            Assert.fail("Connection failed!");
        }
        System.out.println("Connection class is " + connection.getClass().getName() + ".");
        
        // Validate connection.
        boolean isValid = false;
        try {isValid = connection.isValid(5);}
        catch (SQLException e) {
            e.printStackTrace();
            Assert.fail("isValid() failed!");
        }
        Assert.assertTrue(isValid, "Connection is not valid!");
        
        // Close the transaction.
        try {connection.commit();}
        catch (SQLException e) {
            e.printStackTrace();
            Assert.fail("commit() failed!");
        }
        
        // Save the test pool data source so we can close it down
        // in the main routine to guarantee that no connections to
        // the test db exist when we try to drop it.
        if (TEST_POOL.equals(poolName)) _testDataSource = ds;        
        
        return connection;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* dropDB:                                                                      */
    /* ---------------------------------------------------------------------------- */
    private void dropDB(Connection adminConn) throws SQLException
    {
        // This is how one runs data non-sql statement outside of transactions.
        adminConn.setAutoCommit(true);
        
        // Drop the db.
        Statement stmt = adminConn.createStatement();
        stmt.execute("DROP DATABASE IF EXISTS " + TEST_DB);
        stmt.close();
        
        // Reset the connection.
        adminConn.setAutoCommit(false);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* createDB:                                                                    */
    /* ---------------------------------------------------------------------------- */
    private void createDB(Connection adminConn) throws SQLException
    {
        // This is how one runs data non-sql statement outside of transactions.
        adminConn.setAutoCommit(true);
        
        Statement stmt = adminConn.createStatement();
        String sql = "CREATE DATABASE " + TEST_DB +
                     " WITH OWNER " + USER +
                     " ENCODING='UTF8' LC_COLLATE='en_US.utf8' LC_CTYPE='en_US.utf8'";
        stmt.execute(sql);
        stmt.close();
        
        // Reset the connection.
        adminConn.setAutoCommit(false);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* createTable:                                                                 */
    /* ---------------------------------------------------------------------------- */
    private void createTable(Connection testConn) throws SQLException
    {
        Statement stmt = testConn.createStatement();
        String sql = "CREATE TABLE " + TABLE_NAME +
                     "(num integer NOT NULL, string varchar(40))";
        stmt.execute(sql);
        stmt.close();
        testConn.commit();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* InsertIntoTable:                                                             */
    /* ---------------------------------------------------------------------------- */
    private void InsertIntoTable(Connection testConn) throws SQLException
    {
        // Put 2 rows in the table.
        Statement stmt = testConn.createStatement();
        String sql = "INSERT INTO " + TABLE_NAME +
                     " (num, string)" +
                     " VALUES (1, 'hello'), (2, NULL)";
        stmt.executeUpdate(sql);
        stmt.close();
        testConn.commit();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* readTable:                                                                   */
    /* ---------------------------------------------------------------------------- */
    private void readTable(Connection testConn) throws SQLException
    {
        // Query both rows
        Statement stmt = testConn.createStatement();
        String sql = "SELECT * FROM " + TABLE_NAME + 
                     " ORDER BY num";
        ResultSet rs = stmt.executeQuery(sql);
        
        // Check the number of rows.
        int count = 0;
        while (rs.next()) count++;
        Assert.assertEquals(count, 2, 
           "Failed to retrieve the expected number of rows from " + TABLE_NAME + ".");
        
        // Complete the transaction.
        rs.close();
        stmt.close();
        testConn.commit();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* dropTable:                                                                   */
    /* ---------------------------------------------------------------------------- */
    private void dropTable(Connection testConn) throws SQLException
    {
        // Drop the table.
        Statement stmt = testConn.createStatement();
        String sql = "DROP TABLE " + TABLE_NAME;
        stmt.executeUpdate(sql);
        stmt.close();
        testConn.commit();
    }
}
