package edu.utexas.tacc.tapis.shareddb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import com.zaxxer.hikari.HikariDataSource;

import edu.utexas.tacc.tapis.shareddb.datasource.HikariDSGenerator;

// ******* THIS TEST HAS NOT BEEN PORTED TO TAPIS YET (IT STILL USES MYSQL) *******

/** This test is expected to run on a machine with a default timezone that matches
 * that of the MySQL server (Americas/Chicago).   
 * 
 * @author rcardone
 */
@Test(groups={"integration"})
public class DatetimeTest 
{
	/* **************************************************************************** */
	/*                                  Constants                                   */
	/* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(DatetimeTest.class);
    
	// Hikari configuration parameters.
	private static final String  JDBC_URL = "jdbc:mysql://localhost:3306/agave-api";
	private static final String  USER = "agaveapi";
	private static final String  PASSWORD = "d3f@ult$";
	private static final int     MAX_POOL_SIZE = 2;
	
	/* **************************************************************************** */
	/*                                   Fields                                     */
	/* **************************************************************************** */
	private HikariDataSource _ds;
	private Connection       _conn;
	
	/* **************************************************************************** */
	/*                               Test Setup/Teardown                            */
	/* **************************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* setup:                                                                 */
    /* ---------------------------------------------------------------------- */
    @BeforeSuite
    private void setup()
    {
		// Get an Hikari data source using the multi-argument constructor.
		HikariDSGenerator dsgen = new HikariDSGenerator();
		_ds = dsgen.getDataSource(getClass().getSimpleName(), "testDS2-pool",  
		                          JDBC_URL, USER, PASSWORD, MAX_POOL_SIZE);
        
        // Get the connection.
        try {_conn = _ds.getConnection();}
		catch (SQLException e) {
			e.printStackTrace();
			Assert.fail("Connection failed!");
		}
    }
	
    /* ---------------------------------------------------------------------- */
    /* teardown:                                                              */
    /* ---------------------------------------------------------------------- */
    @AfterSuite
    private void teardown()
    {
        // Close the datasource and its pool.
    	if (_conn != null)
			try {_conn.close();} catch (SQLException e) {e.printStackTrace();}
        if (_ds != null) _ds.close();
    }
    
	/* **************************************************************************** */
	/*                                    Tests                                     */
	/* **************************************************************************** */
	/* ---------------------------------------------------------------------------- */
	/* testDatetime:                                                                */
	/* ---------------------------------------------------------------------------- */
	@Test(enabled=true)
	public void testDatetime()
	{
		// Control the deletion of the temporary table in case of error.
		boolean keepTable = false;
		
		// Quietly Clean up from previous run if necessary.
		try {
			Statement stmt = _conn.createStatement();
			String sql = "DROP TABLE IF EXISTS `test_datetime`";
			stmt.execute(sql);
			_conn.commit();
		}
		catch (Exception e) {}
		
		// Create table and insert a bunch of dates and times.
		try {
			// Create table.
			String sql = "CREATE TABLE IF NOT EXISTS `test_datetime` ( " +
		            	 "`id` bigint(20) NOT NULL AUTO_INCREMENT, " +
					     "`desc` varchar(32), " +
		            	 "`dt` datetime(3) NOT NULL, " +
		            	 "`ts` timestamp(3) NOT NULL, " +
		            	 "PRIMARY KEY (`id`) " +
		            	 ") CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;";
		
			// Insert rows
			Statement stmt = _conn.createStatement();
			stmt.execute(sql);
			stmt.close();
			_conn.commit();
			
			// Get the current timezone offset.
			ZoneOffset tzOffset = OffsetDateTime.now().getOffset();
			_log.info("** tzOffset = " + tzOffset);
			ZoneId tzId = ZoneId.systemDefault();
			_log.info("** tzId = " + tzId);
			
			// Create instants.
			Instant instant1 = Instant.now();
			_log.info("** Instant.now() = " + instant1.toString());
			Instant instant2 = Instant.now(Clock.systemUTC());
			_log.info("** Instant.now(Clock.systemUTC()) = " + instant2.toString());
			Instant instant3 = Instant.now(Clock.systemDefaultZone());
			_log.info("** Instant.now(Clock.systemDefaultZone()) = " + instant3.toString());
			
			OffsetDateTime odt = instant1.atOffset(ZoneOffset.UTC );
			_log.info("** instant.atOffset(ZoneOffset.UTC) =  " + odt);
			
			// Create local date-time objects.
			LocalDateTime localdt1 = LocalDateTime.ofInstant(instant1, ZoneId.of(ZoneOffset.UTC.getId()));
			_log.info("** LocalDateTime.ofInstant(instant1, ZoneId.of(ZoneOffset.UTC.getId())) =  " + localdt1);
			LocalDateTime localdt2 = LocalDateTime.ofInstant(instant1, ZoneId.of("Z"));
			_log.info("** LocalDateTime.ofInstant(instant1, ZoneId.of(\"Z\")) =  " + localdt2);
			LocalDateTime localdt3 = LocalDateTime.ofInstant(instant1, ZoneId.systemDefault());
			_log.info("** LocalDateTime.ofInstant(instant1, ZoneId.systemDefault() =  " + localdt3);
			
//			Instant instant = Instant.parse("2017-07-31T16:16:28.126Z");
			
			// Convert to sql types.
			java.sql.Timestamp tsInstant1 = java.sql.Timestamp.from(instant1);
			java.sql.Timestamp tsInstant2 = java.sql.Timestamp.from(instant2);
			java.sql.Timestamp tsInstant3 = java.sql.Timestamp.from(instant3);
//			java.sql.Timestamp jdt = java.sql.Timestamp.from(localdt.atOffset(ZoneOffset.ofHours(0)).toInstant());
			java.sql.Timestamp tsLocaldt1 = java.sql.Timestamp.valueOf(localdt1);
			java.sql.Timestamp tsLocaldt2 = java.sql.Timestamp.valueOf(localdt2);
			java.sql.Timestamp tsLocaldt3 = java.sql.Timestamp.valueOf(localdt3);
			
			// Assign insertion statement.
			sql = "INSERT INTO `test_datetime` (`desc`, `dt`, `ts`) VALUES (?, ?, ?)";
			
			// -- tsInstant1
			PreparedStatement pstmt = _conn.prepareStatement(sql);
			pstmt.setString(1, "tsInstant1");
			pstmt.setTimestamp(2, tsInstant1);
			pstmt.setTimestamp(3, tsInstant1);
			pstmt.executeUpdate();
			pstmt.close();
			
			// -- tsInstant2
			pstmt = _conn.prepareStatement(sql);
			pstmt.setString(1, "tsInstant2");
			pstmt.setTimestamp(2, tsInstant2);
			pstmt.setTimestamp(3, tsInstant2);
			pstmt.executeUpdate();
			pstmt.close();
			
			// -- tsInstant3
			pstmt = _conn.prepareStatement(sql);
			pstmt.setString(1, "tsInstant3");
			pstmt.setTimestamp(2, tsInstant3);
			pstmt.setTimestamp(3, tsInstant3);
			pstmt.executeUpdate();
			pstmt.close();
			
			// -- tsLocaldt1
			pstmt = _conn.prepareStatement(sql);
			pstmt.setString(1, "tsLocaldt1");
			pstmt.setTimestamp(2, tsLocaldt1);
			pstmt.setTimestamp(3, tsLocaldt1);
			pstmt.executeUpdate();
			pstmt.close();
			
			// -- tsLocaldt2
			pstmt = _conn.prepareStatement(sql);
			pstmt.setString(1, "tsLocaldt2");
			pstmt.setTimestamp(2, tsLocaldt2);
			pstmt.setTimestamp(3, tsLocaldt2);
			pstmt.executeUpdate();
			pstmt.close();
			
			// -- tsLocaldt3
			pstmt = _conn.prepareStatement(sql);
			pstmt.setString(1, "tsLocaldt3");
			pstmt.setTimestamp(2, tsLocaldt3);
			pstmt.setTimestamp(3, tsLocaldt3);
			pstmt.executeUpdate();
			pstmt.close();
			
			_conn.commit();
			
			// Retrieve rows.
			sql = "SELECT * from `test_datetime` ORDER BY id";
			stmt = _conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			boolean hasMore = rs.first();
			while (hasMore) {
				long id = rs.getLong(1);
				String desc = rs.getString(2);
				java.sql.Timestamp dt = rs.getTimestamp(3);
				java.sql.Timestamp ts = rs.getTimestamp(4);
				_log.info("id = " + id + ", desc = " + desc + ", dt = " + dt + ", ts = " + ts);
				
				// Prepare for failure by setting the table deletion flag.
				keepTable = true;
				checkTimes(instant1, desc, dt, ts);
				keepTable = false;
				
				hasMore = rs.next();
			}
			rs.close();
			stmt.close();
			_conn.commit();
		}
		catch(SQLException e) {
			keepTable = true;
			e.printStackTrace();
			Assert.fail("Test failed!");
		}
		finally {
			// Try to clean up on success.
			if (!keepTable)
				try {
					Statement stmt = _conn.createStatement();
					String sql = "DROP TABLE IF EXISTS `test_datetime`";
					stmt.execute(sql);
					_conn.commit();
				}
				catch (Exception e) {e.printStackTrace();}
		}
	}
	
    /* ---------------------------------------------------------------------------- */
	/* checkTimes:                                                                  */
	/* ---------------------------------------------------------------------------- */
	/** Check the times retrieved from the database. 
	 * 
	 * @param instant1 the first (i.e., earliest) instant created in this test
	 * @param desc the name of the test values
	 * @param dt the timestamp from the sql datatime column
	 * @param ts dt the timestamp from the sql timestamp column
	 */
	private void checkTimes(Instant instant1, String desc, 
	                        java.sql.Timestamp dt, java.sql.Timestamp ts)
	{
		  // Converse timestamps from database to instants.
		  Instant dtInstant = dt.toInstant();
		  Instant tsInstant = ts.toInstant();
//		  _log.info("** dtInstant = " + dtInstant + ", tsInstant =  " + tsInstant);
		  
		  // There should be no difference in the retrieved values because (1) SQL 
		  // timestamp gets saved as UTC and converted to SQL server time on retrieval, 
		  // (2) datetime is saved as is using the local timezone of the test machine, 
		  // and (3) the SQL server timezone is the same as the timezone of the machine 
		  // running this test.
		  Assert.assertEquals(dtInstant, tsInstant, 
				              "Expected SQL timestamp and datetime values to be equal.");
		  
		  // Use milliseconds values to handle small differences in instants 
		  // that were created sequentially.
		  long instant1Millis  = instant1.toEpochMilli();
		  long dtInstantMillis = dtInstant.toEpochMilli();
		  
		  // We expect some results to be in local time and some to be UTC.
		  switch (desc)
		  {
		  	// These cases are expected to store their timestamp in UTC and return UTC.
		    // Since UTC is 5 or 6 hours ahead of central time, we expect the local time
		    // instant to be before the value returned from the database.  We allow a
		    // 10ms buffer to account for different root instants.
		  	case "tsLocaldt1":
		  	case "tsLocaldt2":
		  		Assert.assertTrue(dtInstantMillis > instant1Millis + 10, 
		  				"Expected " + desc + " value to be saved as UTC and strictly after local time instant.");
		  		break;
		  		
		    // All other cases return the local time from the database whether that's 
		    // because MySQL converts saved UTC time to local (server) time as in the case 
		  	// of timestamp or if we saved local time as in the case of datetime.  We
		    // ignore differences of less than 10ms.
		  	default:
		  		Assert.assertTrue(instant1Millis - dtInstantMillis < 10, 
		  				"Expected " + desc + " value to match the local time instant.");
		  		break;
		  }
	}
}
