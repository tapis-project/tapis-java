package edu.utexas.tacc.tapis.shareddb.datasource;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.zaxxer.hikari.HikariDataSource;

import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** This file wraps the Hikari connection pool library with factory and convenience methods
 * tailored for use with the postgres JDBC driver for MySql.  The Hikari data source
 * constructors that take a config or properties do not allow JDBC setting to be
 * assigned after construction.  So for complete flexibility, we create an empty
 * data source and allow all configuration methods on it to be called before the
 * data source is used.  The set*() methods provide some convenience.  
 * 
 * Some of the performance and reliability oriented Hikari options are listed here
 * for easy lookup:
 * 
 * 	Setting					Default          Description
 *  -------                 -------          -----------
 * 	connectionTimeout       30 secs          max time to wait for a connection from pool before exception
 *  minimumIdle             maximumPoolSize  number of idle connections allowed (by default no effect)
 *  idleTimeout             10 mins          no effect when minimumIdle == maximumPoolSize (the default)
 *  maxLifetime             30 mins          connection lifetime, should be less than database connection timeout
 *  validationTimeout       5  secs          max time for liveness test, must be < connectionTimeout
 *  leakDetectionThreshold  0  ms            max time for a connection to be checked out before logging a message  
 * 
 * @author rcardone
 *
 */
public class HikariDSGenerator 
{
	/* **************************************************************************** */
	/*                                   Constants                                  */
	/* **************************************************************************** */
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(HikariDSGenerator.class);
    
  // The Tapis database names and schemas.
  public static final String  TAPIS_DB_NAME = "tapisdb";
  public static final String  TAPIS_SEC_DB_NAME = "tapissecdb";
  public static final String  TAPIS_SCHEMA_NAME = "public";
  public static final String  TAPIS_SEC_SCHEMA_NAME = "public";
  
  // Other database defaults.
  public static final String  DEFAULT_DBMS_NAME = "postgresql";
  public static final String  DEFAULT_DRIVER_CLASS_NAME = "org.postgresql.Driver";
  public static final boolean DEFAULT_AUTO_COMMIT = false;
	
  /* **************************************************************************** */
  /*                                    Enums                                     */
  /* **************************************************************************** */
	// The supported output targets for metrics reporting.
	public enum MetricReporterType {SL4J, CONSOLE}
	
  /* **************************************************************************** */
  /*                                    Fields                                    */
  /* **************************************************************************** */
	// The database management system serviced by this driver object.
	private String _dbmsName;
	private String _dbmsDriverName;
	
  /* **************************************************************************** */
  /*                                 Constructors                                 */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* constructor:                                                                 */
  /* ---------------------------------------------------------------------------- */
	/** Default aloe constructor. */
	public HikariDSGenerator()
	{
		_dbmsName = DEFAULT_DBMS_NAME;
		_dbmsDriverName = DEFAULT_DRIVER_CLASS_NAME;
	}
	
  /* ---------------------------------------------------------------------------- */
  /* constructor:                                                                 */
  /* ---------------------------------------------------------------------------- */
	/** Customizable Hikari data source generator.
	 * 
	 * @param dbmsName the name of the database management system appropriate 
	 * 			for use in JDBC URLs
	 * @param dbmsDriverName the fully qualified name of the JDBC driver class
	 */
	public HikariDSGenerator(String dbmsName, String dbmsDriverName)
	{
		// Check inputs.
		if (StringUtils.isBlank(dbmsName)) {
    		String msg = MsgUtils.getMsg("DB_NULL_DB_NAME");
    		_log.error(msg);
    		throw new TapisRuntimeException(msg);
		}
		if (StringUtils.isBlank(dbmsDriverName)) {
    		String msg = MsgUtils.getMsg("DB_NULL_DB_DRIVER_NAME");
    		_log.error(msg);
    		throw new TapisRuntimeException(msg);
		}
		
		// Assign inputs.
		_dbmsName = dbmsName;
		_dbmsDriverName = dbmsDriverName;
	}
	
  /* **************************************************************************** */
  /*                              Data Source Methods                             */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* getDataSource:                                                               */
  /* ---------------------------------------------------------------------------- */
  /** Get a data source with the minimal default aloe settings.
   * 	
   * @return the default data source.
   */
  public HikariDataSource getDataSource()
  {
  	// Create uninitialized data source.
  	HikariDataSource ds = new HikariDataSource();
    	
    // Set some default values.
    ds.setDriverClassName(_dbmsDriverName);
    ds.setAutoCommit(DEFAULT_AUTO_COMMIT);
    return ds;
  }

  /* ---------------------------------------------------------------------------- */
  /* getDataSource:                                                               */
  /* ---------------------------------------------------------------------------- */
  /** Create a data source with the minimal default aloe settings and user input.
   * 
   * @param poolName - a user chosen name for this pool, preferably unique
   * @param jdbcUrl - the jdbc url, such as "jdbc:mysql://localhost:3306/agave-api"
   * @param user - the database user's name 
   * @param password - the database user's password
   * @param maxPoolSize - the size of the connection pool
   * 
   * @return the default data source.
   */
  public HikariDataSource getDataSource(String appName, String poolName, String jdbcUrl, 
      		                            String user, String password, int maxPoolSize)
  {
  	// Check input.
    if (StringUtils.isBlank(appName)) {
        String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getDataSource", "appName");
        _log.error(msg);
       throw new TapisRuntimeException(msg);
    }
  	if (StringUtils.isBlank(poolName)) {
  	  String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getDataSource", "poolName");
  		_log.error(msg);
  		throw new TapisRuntimeException(msg);
  	}
  	if (StringUtils.isBlank(jdbcUrl)) {
  	  String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getDataSource", "jdbcUrl");
  		_log.error(msg);
  		throw new TapisRuntimeException(msg);
  	}
  	if (StringUtils.isBlank(user)) {
  	  String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getDataSource", "user");
   		_log.error(msg);
   		throw new TapisRuntimeException(msg);
   	}
   	if (StringUtils.isBlank(password)) {
   	 String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getDataSource", "password");
   		_log.error(msg);
   		throw new TapisRuntimeException(msg);
   	}
   	if (maxPoolSize < 1) {
   	    String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "getDataSource", maxPoolSize);
   		_log.error(msg);
   		throw new TapisRuntimeException(msg);
   	}
    	
   	// Create data source and configure it with the user's input.
  	HikariDataSource ds = getDataSource();  // sets autocommit default
   	ds.setPoolName(poolName);
    ds.setJdbcUrl(jdbcUrl);
    ds.addDataSourceProperty("user", user);
    ds.addDataSourceProperty("password", password);
    ds.addDataSourceProperty("ApplicationName", appName);
    ds.setMaximumPoolSize(maxPoolSize);
        
  	return ds;
  }

  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* setReliabilityOptions:                                                       */
  /* ---------------------------------------------------------------------------- */
  /** Configure an existing data source's JDBC driver for reliability in our
   * environment. 
   * 
   * @param ds an existing data source
   * @return this object
   */
  public HikariDSGenerator setReliabilityOptions(HikariDataSource ds)
  {
  	// Configure the MariaDB driver for logging.  
   	// See https://mariadb.com/kb/en/mariadb/about-mariadb-connector-j.
    ds.setLeakDetectionThreshold(1200000);  // 20 minutes
   	return this;
  }

  /* ---------------------------------------------------------------------------- */
  /* setMetricRegistry:                                                           */
  /* ---------------------------------------------------------------------------- */
  /** Configure a metrics registry and reporter for the data source.
   * 
   * @param ds the data source
   * @param minutes the reporting interval in minutes
   * @param reporterType the output target for the metrics report
   * @return this object
   */
  public HikariDSGenerator setMetricRegistry(HikariDataSource ds, int minutes,
      		                                 MetricReporterType reporterType) 
  {
    // Don't turn on metrics reporting if the period is zero or less.
    if (minutes <= 0) return this;
    
  	// Create the metrics registry.
    MetricRegistry metrics = new MetricRegistry();
    ds.setMetricRegistry(metrics);
        
    // Activate a reporter.
    switch (reporterType)
    {
     	case SL4J:
     	{
     		// We use this class's static logger.
     		final Slf4jReporter reporter = Slf4jReporter.forRegistry(metrics)
        				.outputTo(_log)
        				.convertRatesTo(TimeUnit.SECONDS)
        				.convertDurationsTo(TimeUnit.MILLISECONDS)
        				.build();
     		reporter.start(minutes, TimeUnit.MINUTES);
     		break;
     	}
     	case CONSOLE:
     	{
     	    final ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
           		       .convertRatesTo(TimeUnit.SECONDS)
           		       .convertDurationsTo(TimeUnit.MILLISECONDS)
           		       .build();
     	    reporter.start(minutes, TimeUnit.MINUTES);
     	    break;
     	}
    }

   return this;
  }
    
  /* ---------------------------------------------------------------------------- */
  /* getJdbcUrl:                                                                  */
  /* ---------------------------------------------------------------------------- */
  /** Construct the JDBC url string from this object's DBMS name field and user input.
   * 
   * @param host the host where the DBMS is running
   * @param port the DBMS's port
   * @param db the name of the database to access
   * @return a string of the form "jdbc:dbmsName://host:port/db"
   */
  public String getJdbcUrl(String host, int port, String db)
  {
  	return "jdbc:" + _dbmsName + "://" + host + ":" + port + "/" + db;
  }
}
