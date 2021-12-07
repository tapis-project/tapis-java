package edu.utexas.tacc.tapis.misc.jobtype;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariDataSource;

import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shareddb.datasource.HikariDSGenerator;

/** This class provides a human-in-the-loop migration of the Jobs database schema
 * defined in V002__AddJobType.sql.  After the flyway migration runs and the job_type
 * column has been added to the jobs table, job_type=FORK for all jobs.  Run this
 * program to update the job_type to BATCH based on the App.appType field that used
 * to maintain this information.  The process is as follows:
 * 
 *  1. Issue this call against the Apps database and export the results:
 *      SELECT DISTINCT tenant, id, latest_version, app_type FROM apps
 *      ORDER BY tenant, id, latest_version, app_type;
 *      
 *  2. Import the information from Apps by manually assigning elements in the
 *     _appVer2JobType map in the initAppVer2JobType() method.
 *  3. Run this program and point it at the target Jobs database using command-line
 *     arguments defined in MigrateJobTypeParms.
 * 
 * @author rcardone
 */
public class MigrateJobType 
{
    /* **************************************************************************** */
    /*                                  Constants                                   */
    /* **************************************************************************** */
    // Connection pool name.
    private static final String DATASOURCE_NAME = "MigrateJobTypePool";
    
    // Job types as strings.
    private static final String FORK  = "FORK";
    private static final String BATCH = "BATCH";
    
    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    // Local logger.
    private static Logger _log = LoggerFactory.getLogger(MigrateJobType.class);
    
    // Migration parameters.
    private MigrateJobTypeParms _parms;

    // The connection pool.
    private HikariDataSource    _dataSource;
    
    // Cache of versioned apps to jobTypes.
    private final HashMap<AppVer,String> _appVer2JobType = initAppVer2JobType();
    
    // Job array with initial capacity specified.
    private final List<JobRecord>        _jobRecords = new ArrayList<>(300);
    
    // List of applications referenced by a job but not in _appVer2JobType;
    private final ArrayList<String>      _missingApps = new ArrayList<>();
    
    // Number of jobs whose jobTypes were updated.
    private int _updates;
    
    // Number of jobs referencing missing applications.
    private int _skippedJobs;
    
    /* **************************************************************************** */
    /*                                 Public Methods                               */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* main:                                                                        */
    /* ---------------------------------------------------------------------------- */
    public static void main(String[] args) throws Exception 
    {
        // Initial log message.
        say("-- MigrateJobType starting.");
        MigrateJobType migrate = new MigrateJobType();
        migrate.execute(args);
        say("-- MigrateJobType stopping.");
    }

    /* **************************************************************************** */
    /*                                Private Methods                               */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* execute:                                                                     */
    /* ---------------------------------------------------------------------------- */
    private void execute(String[] args) throws Exception
    {
        // ------------------------ Initialization ----------------------
        // Parse and validate the command line parameters.
        _parms = getParms(args);
        
        // Create the connection pool.  We exit from here if
        // an exception is thrown.
        _dataSource = getDataSource();

        // ------------------------ Get App References ------------------
        // Check for missing applications.
        checkReferencedApps();
        
        // ------------------------ Retrieve Jobs -----------------------
        // Get a record for each existing job.
        populateJobRecords();
        
        // Determine the job type for each job.
        processJobType();
        
        // Print results.
        reportResults();
    }
    
    /* ---------------------------------------------------------------------------- */
    /* checkReferencedApps:                                                         */
    /* ---------------------------------------------------------------------------- */
    private void checkReferencedApps() throws SQLException
    {
        // Connect to the administrative db.
        Connection conn = null;
        Statement stmt = null;
        try {   
           // Get a connection.
           conn = _dataSource.getConnection();
           stmt = conn.createStatement();
           conn.setAutoCommit(true);
           
           // Drop the tapisdb database if it exists.
           String sql = "SELECT DISTINCT tenant, app_id, app_version FROM jobs ORDER BY tenant, app_id, app_version";
           
           // Execute the query and get the number of rows returned.
           ResultSet rs = stmt.executeQuery(sql);
           
           // Populate result list.
           if (rs != null)
               while (rs.next()) {
                   var rec = new AppVer(rs.getString(1), rs.getString(2), rs.getString(3));
                   if (!_appVer2JobType.containsKey(rec)) _missingApps.add(rec.toString());
               }
         }
         finally 
          {
           // Close the statement.
           if (stmt != null)
              try {stmt.close();}
               catch (SQLException e) 
                {
                 // Log and discard.
                 _log.error(e.getMessage(), e);
                }

           // Close connection.
           if (conn != null)
               try {conn.close();} 
                catch (SQLException e) 
                 {
                  // Log and discard.
                  _log.error(e.getMessage(), e);
                 }   
          }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* populateJobRecords:                                                          */
    /* ---------------------------------------------------------------------------- */
    private void populateJobRecords() throws SQLException
    {
        // Connect to the administrative db.
        Connection conn = null;
        Statement stmt = null;
        try {   
           // Get a connection.
           conn = _dataSource.getConnection();
           stmt = conn.createStatement();
           conn.setAutoCommit(true);
           
           // Drop the tapisdb database if it exists.
           String sql = "SELECT tenant, id, app_id, app_version, job_type FROM jobs ORDER BY tenant, id";
           
           // Execute the query and get the number of rows returned.
           ResultSet rs = stmt.executeQuery(sql);
           
           // Populate result list.
           if (rs != null)
               while (rs.next()) {
                   var rec = new JobRecord();
                   rec.tenant = rs.getString(1);
                   rec.id = rs.getInt(2);
                   rec.appId = rs.getString(3);
                   rec.appVersion = rs.getString(4);
                   rec.jobType = rs.getString(5);
                   _jobRecords.add(rec);
               }
         }
         finally 
          {
           // Close the statement.
           if (stmt != null)
              try {stmt.close();}
               catch (SQLException e) 
                {
                 // Log and discard.
                 _log.error(e.getMessage(), e);
                }
           
           // Close connection.
           if (conn != null)
               try {conn.close();} 
                catch (SQLException e) 
                 {
                  // Log and discard.
                  _log.error(e.getMessage(), e);
                 }   
          }
    }

    /* ---------------------------------------------------------------------------- */
    /* updateJobType:                                                               */
    /* ---------------------------------------------------------------------------- */
    private void updateJobType(int jobId, String jobType) throws SQLException
    {
        // Connect to the administrative db.
        Connection conn = null;
        Statement stmt = null;
        try {   
           // Get a connection.
           conn = _dataSource.getConnection();
           stmt = conn.createStatement();
           conn.setAutoCommit(true);
           
           // Drop the tapisdb database if it exists.
           String sql = "UPDATE jobs SET job_type = '" + jobType + "' WHERE id = " + jobId;
           
           // Execute the query and get the number of rows returned.
           stmt.executeUpdate(sql);           
         }
         finally 
          {
           // Close the statement.
           if (stmt != null)
              try {stmt.close();}
               catch (SQLException e) 
                {
                 // Log and discard.
                 _log.error(e.getMessage(), e);
                }
           
           // Close connection.
           if (conn != null)
               try {conn.close();} 
                catch (SQLException e) 
                 {
                  // Log and discard.
                  _log.error(e.getMessage(), e);
                 }   
          }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* processJobType:                                                              */
    /* ---------------------------------------------------------------------------- */
    private void processJobType() throws SQLException
    {
        for (var rec : _jobRecords) {
            // Do nothing if the default value has been changed.
            if (BATCH.equals(rec.jobType)) continue;
            
            // See if we know how to change this job's type.
            var appVer = new AppVer(rec.tenant, rec.appId, rec.appVersion);
            var jobType = _appVer2JobType.get(appVer);
            if (jobType == null) {
                _skippedJobs++;
                continue;
            }
            
            // See if different than the default value.
            if (FORK.equals(jobType)) continue;
            
            // Change the jobType in the job record.
            updateJobType(rec.id, jobType);
            _updates++;
        }
    }
    
    /* ---------------------------------------------------------------------------- */
    /* reportResults:                                                               */
    /* ---------------------------------------------------------------------------- */
    private void reportResults()
    {
        say("Job records retrieved:   " + _jobRecords.size());
        say("Job type values changed: " + _updates);
        say("Jobs using missing apps: " + _skippedJobs);
        
        // Report all the applications whose jobType was not processed.
        if (!_missingApps.isEmpty()) say("\n");
        for (var s : _missingApps) say("*** MISSING APP: " + s);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getParms:                                                                    */
    /* ---------------------------------------------------------------------------- */
    /** Allow subclasses to substitute their own parameter processing in place of 
     * this package's parameter processing.
     * 
     * @param args command line arguments
     * @return the parsed and validated parameters
     * @throws TapisJDBCException
     */
    protected MigrateJobTypeParms getParms(String[] args) 
      throws TapisJDBCException 
    {return new MigrateJobTypeParms(args);}
    
    /* ---------------------------------------------------------------------------- */
    /* getDataSource:                                                               */
    /* ---------------------------------------------------------------------------- */
    private HikariDataSource getDataSource()
    {
       HikariDSGenerator dsgen = new HikariDSGenerator();
       HikariDataSource ds = 
           dsgen.getDataSource(getClass().getSimpleName(), DATASOURCE_NAME, 
                   getJdbcUrl(), _parms.username, _parms.password, 2);
       return ds;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getJdbcUrl:                                                                  */
    /* ---------------------------------------------------------------------------- */
    /** Create a JDBC data source url using the execution input parameters for any 
     * supported SQL database.
     * 
     * @return a url string
     */
    private String getJdbcUrl()
    {
      // This string depends on user input for most components.
      return "jdbc:" + _parms.dbmsName + "://" + _parms.host + ":" + 
             _parms.port + "/" + _parms.dbname;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* initAppVer2JobType:                                                          */
    /* ---------------------------------------------------------------------------- */
    /** This method hardcodes application information for job type in the target
     * environment.  
     * 
     * ============================================================================
     *   CHANGE THE HARDCODED VALUES HERE TO MATCH THE TARGET ENVIRONMENT APPS
     * ============================================================================
     * 
     * @return the filled in map
     */
    private HashMap<AppVer,String> initAppVer2JobType()
    {
        var map = new HashMap<AppVer,String>();
        
        // Manually enter each application/version's record.
        map.put(new AppVer("dev", "JobApp1", "0.0.1"), BATCH);
        map.put(new AppVer("dev", "JobAppWithInput", "0.0.1"), BATCH);
        
        map.put(new AppVer("dev", "SleepSeconds", "0.0.1"), FORK);
        map.put(new AppVer("dev", "SleepSeconds-demo-final", "0.0.1"), FORK);
        map.put(new AppVer("dev", "SleepSeconds-demo-test", "0.0.1"), FORK);
        map.put(new AppVer("dev", "SleepSeconds-J", "0.0.1"), FORK);
        map.put(new AppVer("dev", "SleepSeconds-tutorial", "0.0.1"), FORK);
        
        map.put(new AppVer("dev", "SlurmSleepSecondsVM", "0.0.1"), BATCH);
        map.put(new AppVer("dev", "SlurmWordCountTestVM", "0.0.1"), BATCH);
        
        map.put(new AppVer("dev", "SyRunSleepSeconds", "0.0.1"), FORK);
        map.put(new AppVer("dev", "SyRunSleepSecondsNoIPFiles-2", "0.0.1"), FORK);
        
        map.put(new AppVer("dev", "SyStartSleepSeconds", "0.0.1"), FORK);
        map.put(new AppVer("dev", "SyStartSleepSecondsNoIPFiles", "0.0.1"), FORK);
        map.put(new AppVer("dev", "SyStartSleepSecondsNoIPFiles-1", "0.0.1"), FORK);
        map.put(new AppVer("dev", "SyStartSleepSecondsNoIPFiles-2", "0.0.1"), FORK);
        
        map.put(new AppVer("dev", "tacc-sample-app-testuser2", "0.1"), FORK);
        map.put(new AppVer("dev", "tacc-sample-app-userid", "0.1"), FORK);
        map.put(new AppVer("dev", "TestApp1", "0.0.1"), BATCH);
        
        map.put(new AppVer("tacc", "SlurmSleepSeconds-jdm-test", "0.0.1"), BATCH);
        
        return map;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* say:                                                                         */
    /* ---------------------------------------------------------------------------- */
    private static void say(String s) {System.out.println(s);}

    /* **************************************************************************** */
    /*                                   Apps Class                                 */
    /* **************************************************************************** */
    private static final class AppVer
    {
        private String tenant;
        private String appId;
        private String appVersion;

        private AppVer(String tenantId, String app_id, String app_version) {
            if (tenantId == null || app_id == null || app_version == null) 
                throw new RuntimeException("Null parameter on Apps construction!");
            tenant = tenantId; appId = app_id; appVersion = app_version;
        }
        
        @Override
        public boolean equals(Object that) {
            if (!(that instanceof AppVer)) return false;
            if (tenant.equals(((AppVer)that).tenant) &&
                appId.equals(((AppVer)that).appId) && 
                appVersion.equals(((AppVer)that).appVersion))
                return true;
            return false;
        }
        
        @Override
        public int hashCode() {return toString().hashCode();}
        
        @Override
        public String toString() {return appId + "-" + appVersion + "@" + tenant;}
    }
    
    /* **************************************************************************** */
    /*                                JobRecord Class                               */
    /* **************************************************************************** */
    private static final class JobRecord 
    {
        private String tenant;
        private int    id;
        private String appId;
        private String appVersion;
        private String jobType;
    }
}   
