package edu.utexas.tacc.tapis.migrate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.StringUtils;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariDataSource;

import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shareddb.datasource.HikariDSGenerator;

/** This utility program migrates the TAPIS_DB_NAME using Flyway.  The options for this 
 * program are implemented in MigrateParms, which can be viewed by running this program 
 * with the -help option.
 * 
 * The clean and drop options should not be used in production since they will delete
 * all data in the database.
 * 
 * If the TAPIS_DB_NAME database does not exist, this program will create it and grant 
 * the TAPIS_USER user ALL privileges on it.  If the TAPIS_USER user does not exist,
 * the database will not be created and processing will abort.   
 * 
 * Note that any administrative user--one that can create databases--can be used to
 * connect to the database.
 * 
 * @author rcardone
 *
 */
public class TapisJDBCMigrate
{
  /* **************************************************************************** */
  /*                                  Constants                                   */
  /* **************************************************************************** */
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(TapisJDBCMigrate.class);
    
  // The userid that services use.
  private static final String TAPIS_USER = "tapis"; 
  private static final String DFT_TAPIS_USER_PASSWORD = "password"; // change on 1st use
  
  // The schema search path for the tapis user and database.
  private static final String SEARCH_PATH = "public";
  
  // The administrative database name used to query metadata.
  private static final String ADMIN_DB_NAME = "postgres";
	
  // Connection pool name.
  private static final String DATASOURCE_NAME = "TapisJDBCMigratePool";
  
  /* **************************************************************************** */
  /*                                    Fields                                    */
  /* **************************************************************************** */
  // The tapis database name used throughout this program.
  private final String _tapisDbName;
  
  // Parse and validate command line input parameters.
  private TapisJDBCMigrateParms _parms;
  
  // The connection pool.
  private HikariDataSource _dataSource;

  /* **************************************************************************** */
  /*                                 Constructors                                 */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* constructor:                                                                 */
  /* ---------------------------------------------------------------------------- */
  protected TapisJDBCMigrate(String dbName) throws TapisJDBCException
  {
      // Make sure we have a reasonable database name.
      if (StringUtils.isBlank(ADMIN_DB_NAME)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "TapisJDBCMigrate", "dbName");
          _log.error(msg);
          throw new TapisJDBCException(msg);
      }
      
      // Assign the name.
      _tapisDbName = dbName;
  }
  
  /* **************************************************************************** */
  /*                                 Public Methods                               */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* main:                                                                        */
  /* ---------------------------------------------------------------------------- */
  /** The standard command line invocation method.
   * 
   * @param args - the arguments defined in MigrateParms and processed by Args4J.
   */
  public static void main(String[] args)
      throws TapisJDBCException
  {
    // Initial log message.
    _log.info(MsgUtils.getMsg("MIGRATE_STARTING"));
    TapisJDBCMigrate migrate = new TapisJDBCMigrate(HikariDSGenerator.TAPIS_DB_NAME);
    migrate.execute(args);
    _log.info(MsgUtils.getMsg("MIGRATE_STOPPING"));
  }

  /* ---------------------------------------------------------------------------- */
  /* execute:                                                                     */
  /* ---------------------------------------------------------------------------- */
  /** The main driver instance method.  This method consumes the same parameter
   * as main().  It's essential responsibilities include:
   * 
   *    - Parse and validate the input parameters
   *    - Load the specified JDBC driver class
   *    - Conditionally drop the tapisdb database
   *    - Conditionally clean (remove all content) of the aleodb database
   *    - Conditionally create or update the database
   * 
   * @param args - the arguments defined in MigrateParms and processed by Args4J.
   * @throws TapisJDBCException - on error
   */
  public void execute(String[] args)
      throws TapisJDBCException
  {
    // ------------------------ Initialization ----------------------
    // Parse and validate the command line parameters.
    _parms = getParms(args);
    
    // Create the connection pool.  We exit from here if
    // an exception is thrown.
    _dataSource = getAdminDataSource();
    
    // ------------------------ Drop DB -----------------------------
    // Drop tapisdb database if requested (excludes clean).
    if (_parms.isDropDatabases || _parms.isDropOnly)
      {
        _log.info(MsgUtils.getMsg("MIGRATE_DROPPING_DB", _tapisDbName));
        dropTapisDB();
      }
    
    // Was this a DB drop-only operation?
    if (_parms.isDropOnly) return;
    
    // ------------------------ Clean DB ----------------------------
    // Clean tapisdb database if requested (excludes drop).
    if (_parms.isCleanDatabases || _parms.isCleanOnly)
      {
        _log.info(MsgUtils.getMsg("MIGRATE_CLEANING_DB", _tapisDbName));
        cleanTapisDB();
      }
    
    // Was this a DB clean-only operation?
    if (_parms.isCleanOnly) return;
    
    // ------------------------ Create DB ---------------------------
    // Create the tapisdb database if it doesn't exist.
    createTapisDB();
    
    // ------------------------ Process DB --------------------------
    // Call Flyway to keep schema up-to-date.
    if (_parms.doBaseline) baselineTapisDB();
       else migrateTapisDB();
    
    // ------------------------ Release Pool ------------------------
    // Close all connections.
    _dataSource.close();
  }
  
  /* **************************************************************************** */
  /*                             Protected Methods                                */
  /* **************************************************************************** */
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
  protected TapisJDBCMigrateParms getParms(String[] args) 
    throws TapisJDBCException 
  {return new TapisJDBCMigrateParms(args);}
  
  /* **************************************************************************** */
  /*                               Private Methods                                */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* getAdminDataSource:                                                          */
  /* ---------------------------------------------------------------------------- */
  private HikariDataSource getAdminDataSource()
  {
	 HikariDSGenerator dsgen = new HikariDSGenerator();
	 HikariDataSource ds = 
	     dsgen.getDataSource(getClass().getSimpleName(), DATASOURCE_NAME, 
	             getJdbcUrl(ADMIN_DB_NAME), _parms.username, _parms.password, 4);
	 return ds;
  }
  
  /* ---------------------------------------------------------------------------- */
  /* cleanTapisDB:                                                                */
  /* ---------------------------------------------------------------------------- */
  /** Remove the contents of the tapisdb, but leave the database intact.  This 
   * method uses the input parameters and Flyway to perform the actual database 
   * migration.  This method will work with all SQL databases supported by Flyway.
   * 
   * @throws TapisJDBCException on error
   */
  private void cleanTapisDB()
      throws TapisJDBCException
  {
    // Construct the data source targeting the tapisdb database.
    String dataSource = getJdbcUrl(_tapisDbName);
    
    try {  
        // Clean the database using Flyway.  This wipes out all the database
        // content but keeps the database.
        Flyway flyway = Flyway.configure()
                        .dataSource(dataSource, _parms.username, _parms.password)
                        .schemas(_parms.schema)
                        .cleanDisabled(false)
                        .load();
        flyway.clean();
      }
     catch (Exception e)
      {
       // This stops the migration.
       String msg = "Unable to clean " + _tapisDbName + 
                    " with data source \"" + dataSource + "\".";
       _log.error(MsgUtils.getMsg("MIGRATE_CLEANING_FAILED", _tapisDbName, dataSource), e);
       throw new TapisJDBCException(msg, e);
      }
  }
  
  /* ---------------------------------------------------------------------------- */
  /* dropTapisDB:                                                                 */
  /* ---------------------------------------------------------------------------- */
  private void dropTapisDB() 
    throws TapisJDBCException
  {
    // Connect to the administrative db.
    Connection conn = null;
    Statement stmt = null;
    try {   
       // Get a connection to the administrative db and set autocommit on.
       conn = _dataSource.getConnection();
       stmt = conn.createStatement();
       
       // Drop the tapisdb database if it exists.
       String sql = "DROP DATABASE IF EXISTS " + _tapisDbName;
       conn.setAutoCommit(true);
       stmt.executeUpdate(sql);
       if (_log.isInfoEnabled())
         _log.info(MsgUtils.getMsg("MIGRATE_DB_DROPPED", _tapisDbName));
      }
     catch (Exception e)
      {
       // Log and throw wrapper exception.
       String msg = MsgUtils.getMsg("MIGRATE_DROPPING_FAILED", _tapisDbName);
       _log.error(msg, e);
       throw new TapisJDBCException(msg, e);
      }
     finally 
      {
       // Close the statement.
       if (stmt != null)
          try {stmt.close();}
           catch (SQLException e) 
            {
             // Log and discard.
             String msg = MsgUtils.getMsg("MIGRATE_CLOSE_STMT_FAILED", ADMIN_DB_NAME);
             _log.error(msg, e);
            }
         
       // Always try to reset the connection setting.
       try {conn.setAutoCommit(false);}
           catch (SQLException e) {
               // Log and discard.
               String msg = MsgUtils.getMsg("MIGRATE_SET_AUTOCOMMIT_FAILED", ADMIN_DB_NAME);
               _log.error(msg, e);
           }
       
       // Close the connection to the administrative database.
       if (conn != null)
          try {conn.close();} 
           catch (SQLException e) 
            {
             // Log and discard.
             String msg = MsgUtils.getMsg("MIGRATE_CLOSE_CONN_FAILED", ADMIN_DB_NAME);
             _log.error(msg, e);
            }   
      }
  }
  
  /* ---------------------------------------------------------------------------- */
  /* createTapisDB:                                                               */
  /* ---------------------------------------------------------------------------- */
  private void createTapisDB() 
    throws TapisJDBCException
  {
    // Create the tapisdb database if it doesn't exist yet.
    Connection conn = null;
    Statement stmt = null;
    try {  
      // ------------------------- Check DB -------------------------
      // Get a connection to the administrative db and set autocommit on.
      conn = _dataSource.getConnection();
      stmt = conn.createStatement();
       
      // The db discovery query.
      String sql = 
    	  "SELECT datname FROM pg_catalog.pg_database WHERE datname = '" + _tapisDbName + "'";

      // Execute the query and get the number of rows returned.
      ResultSet rs = stmt.executeQuery(sql);
                  
      // Move the cursor to the first result row.
      // There should be exactly one result row.
      boolean hasFirst = rs.next();
         
      // Close the result and statement, commit the transaction.
      rs.close();
      stmt.close();
      conn.commit();
         
      // There's no work to do if the DB exists.
      if (hasFirst) 
         {
          if (_log.isInfoEnabled())
             _log.info(MsgUtils.getMsg("MIGRATE_FOUND_DB", _tapisDbName));
          return;
         }
         
      // ---------------------- Check Tapis User ---------------------
      // Make sure the required user account already exists
      // if we are not logged in as that user.
      if (!TAPIS_USER.equals(_parms.username)) {
    	  sql = "SELECT rolname FROM pg_roles WHERE rolname = '" + TAPIS_USER + "'";
    	  stmt = conn.createStatement();
    	  rs = stmt.executeQuery(sql);
    	  hasFirst = rs.next();
          rs.close();
          stmt.close();
          conn.commit();

          // We can't continue if the tapis user doesn't exist.  We could
          // create the user here, but that would require passing in two
          // sets of credentials to this program.  For now, we leave the
          // user account creation as an off-line process.
          if (!hasFirst) createTapisUser(conn);
            else 
              if (_log.isInfoEnabled())
                  _log.info(MsgUtils.getMsg("MIGRATE_FOUND_USER", TAPIS_USER));
      }
      
      // ------------------------- Create DB -------------------------
      // Create the tapis database.  Once is created, 
      // the database must be explicitly dropped to remove it.
      sql = "CREATE DATABASE " + _tapisDbName +
              " WITH OWNER " + TAPIS_USER +
              " ENCODING='UTF8' LC_COLLATE='en_US.utf8' LC_CTYPE='en_US.utf8'";

      // Autocommit must be set on for this type of statement.
      conn.setAutoCommit(true);
      stmt = conn.createStatement();
      stmt.execute(sql);
      stmt.close();
      conn.setAutoCommit(false);
      
      if (_log.isInfoEnabled())
          _log.info(MsgUtils.getMsg("MIGRATE_DB_CREATED", _tapisDbName));
      
      // ---------------------- Configure DB -------------------------
      // Limit the search to the public schema in the new database.
      sql = "ALTER DATABASE " + _tapisDbName + " SET search_path TO " + SEARCH_PATH; 
      stmt = conn.createStatement();
      stmt.execute(sql);
      stmt.close();
      conn.commit();      

      if (_log.isInfoEnabled())
          _log.info(MsgUtils.getMsg("MIGRATE_DB_SCHEMA_SEARCH", _tapisDbName, SEARCH_PATH));
    }
    catch (Exception e)
    {
      // Not all exceptions mean that the db creation failed.
      String msg = MsgUtils.getMsg("MIGRATE_INCOMPLETE", _tapisDbName);
      _log.error(msg, e);
      throw new TapisJDBCException(msg, e);
     }
    finally 
     {
      if (conn != null) {
          // Always try to reset the connection setting.
          try {conn.setAutoCommit(false);}
          catch (SQLException e) {
              // Log and discard.
              String msg = MsgUtils.getMsg("MIGRATE_SET_AUTOCOMMIT_FAILED", ADMIN_DB_NAME);
              _log.error(msg, e);
          }
        
      // Always close connection to admin db.
      // This also closes any open statement.
         try {conn.close();} 
          catch (SQLException e) 
           {
            // Log and discard.
            String msg = MsgUtils.getMsg("MIGRATE_CONN_FAILED", ADMIN_DB_NAME);
            _log.error(msg, e);
           }   
      }
     }
  }
  
  /* ---------------------------------------------------------------------------- */
  /* createTapisUser:                                                             */
  /* ---------------------------------------------------------------------------- */
  /** Create and configure the tapis user.  We want the user to use the public 
   * schema by default.  
   * 
   * @param conn
   * @throws SQLException 
   */
  private void createTapisUser(Connection conn) throws SQLException
  {
      // Create the tapis user.
      String sql = "CREATE USER " + TAPIS_USER + " PASSWORD '" + 
                   DFT_TAPIS_USER_PASSWORD + "'";
      Statement stmt = conn.createStatement();
      stmt.execute(sql);
      stmt.close();
      conn.commit();
      
      if (_log.isInfoEnabled())
          _log.info(MsgUtils.getMsg("MIGRATE_CREATED_USER", TAPIS_USER));
      
      // Customize the user's search path so that the public schema is used.
      // This affect the user's search path in all databases.
      sql = "ALTER ROLE " + TAPIS_USER + " SET search_path TO " + SEARCH_PATH; 
      stmt = conn.createStatement();
      stmt.execute(sql);
      stmt.close();
      conn.commit();      
      
      if (_log.isInfoEnabled())
          _log.info(MsgUtils.getMsg("MIGRATE_USER_SCHEMA_SEARCH", TAPIS_USER, SEARCH_PATH));
  }
  
  /* ---------------------------------------------------------------------------- */
  /* configureFlyway:                                                             */
  /* ---------------------------------------------------------------------------- */
  /** Create the flyway object used for migration or baselining.
   * 
   * @throws TapisJDBCException on error
   */
  private Flyway configureFlyway()
   throws TapisJDBCException
  {
    // Construct the flyway object targeting the named database.
    String dataSource = getJdbcUrl(_tapisDbName);
    try {  
      Flyway flyway = Flyway.configure()
                      .dataSource(dataSource, _parms.username, _parms.password)
                      .locations(_parms.cmdDirectory.split(","))
                      .schemas(_parms.schema)
                      .load();
      return flyway;
     }
    catch (Exception e)
     {
      // This stops flyway processing.
      String msg = MsgUtils.getMsg("MIGRATE_CONFIG_FAILED", _tapisDbName, dataSource);
      _log.error(msg, e);
      throw new TapisJDBCException(msg, e);
     }
  }
  
  /* ---------------------------------------------------------------------------- */
  /* migrateTapisDB:                                                              */
  /* ---------------------------------------------------------------------------- */
  /** Migrate the tapisdb if it's not up-to-date.  This method uses the input
   * parameters and Flyway to perform the actual database migration.  This method 
   * will work with all SQL databases supported by Flyway.
   * 
   * @throws TapisJDBCException on error
   */
  private void migrateTapisDB()
   throws TapisJDBCException
  {
	// Get the flyway object.
	Flyway flyway = configureFlyway();
	  
    // Construct the data source targeting the named database.
    String dataSource = getJdbcUrl(_tapisDbName);
    try {  
      // Migrate the database using all properly named command files in the 
      // migration file directory.  See the Flyway documentation for command file 
      // naming conventions.
      //
      // This migration utility uses both java and native sql command files.
      // The java command files implement a required flyway interface and 
      // reside in a well-known directory in the source directory subtree.  
      // Similarly, the sql command files reside in the a parallel directory  
      // in the resources directory subtree.  When the application is packaged, 
      // the files in these two directories are combined since they have the 
      // same path name when their roots are removed.  Flyway processes the 
      // java and sql files in the one resultant directory at runtime.
      int count = flyway.migrate();
         
      // Report number of migration files executed.
      if (count > 0)
        _log.info(MsgUtils.getMsg("MIGRATE_SUCCESS", _tapisDbName, count));
     }
    catch (Exception e)
     {
      // This stops the migration.
      String msg = MsgUtils.getMsg("MIGRATE_FAILED", _tapisDbName, dataSource, _parms.cmdDirectory);
      _log.error(msg, e);
      throw new TapisJDBCException(msg, e);
     }
  }
  
  /* ---------------------------------------------------------------------------- */
  /* baselineTapisDB:                                                             */
  /* ---------------------------------------------------------------------------- */
  /** Baseline the existing tapisdb so that flyway can begin migrations from a 
   * non-empty starting point.
   * 
   * @throws TapisJDBCException on error
   */
  private void baselineTapisDB()
   throws TapisJDBCException
  {
	// Get the flyway object.
	Flyway flyway = configureFlyway();
	  
    // Construct the data source targeting the named database.
    String dataSource = getJdbcUrl(_tapisDbName);
    try {  
      // Baseline the existing database.
      flyway.baseline();
     }
    catch (Exception e)
     {
      // This stops the migration.
      String msg = MsgUtils.getMsg("MIGRATE_BASELINING_FAILED", _tapisDbName, dataSource);
      _log.error(msg, e);
      throw new TapisJDBCException(msg, e);
     }
  }
  
  /* ---------------------------------------------------------------------------- */
  /* getJdbcUrl:                                                                  */
  /* ---------------------------------------------------------------------------- */
  /** Create a JDBC data source url using the execution input parameters for any 
   * supported SQL database.
   * 
   * @return a url string
   */
  private String getJdbcUrl(String dbname)
  {
    // This string depends on user input for most components.
    return "jdbc:" + _parms.dbmsName + "://" + _parms.host + ":" + 
           _parms.port + "/" + dbname;
  }
  
}
