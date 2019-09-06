package edu.utexas.tacc.tapis.files.lib.kernel.config;

import java.util.Properties;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.parameters.TapisEnv;
import edu.utexas.tacc.tapis.shared.parameters.TapisInput;
import edu.utexas.tacc.tapis.shared.parameters.TapisEnv.EnvVar;
import edu.utexas.tacc.tapis.shared.uuid.TapisUUID;
import edu.utexas.tacc.tapis.shared.uuid.UUIDType;

/** This class contains the complete and effective set of runtime parameters
 * for this service.  Each service has it own version of this file that
 * contains the resolved values of configuration parameters needed to
 * initialize and run this service alone.  By resolved, we mean the values
 * assigned in this class are from the highest precedence source as
 * computed by TapisInput.  In addition, this class does not contain values 
 * used to initialize services other than the one in which it appears.
 * 
 * The getInstance() method of this singleton class will throw a runtime
 * exception if a required parameter is not provided or if any parameter
 * assignment fails, such as on a type conversion error.  This behavior
 * can be used to fail-fast services that are not configured correctly by
 * calling getInstance() early in a service's initialization sequence.
 * 
 *  @author rcardone 
 */
public class RuntimeParameters {
	 /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(RuntimeParameters.class);
    
 // Parameter defaults.
    private static final int CONNECTION_POOL_SIZE = 10;
  
    // Maximum size of a instance name string.
    private static final int MAX_INSTANCE_NAME_LEN = 26;
  
    // Default database metering interval in minutes.
    private static final int DEFAULT_DB_METER_INTERVAL_MINUTES = 60 * 24;
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // Globally unique id that identifies this JVM instance.
    private static final TapisUUID id = new TapisUUID(UUIDType.JOB); 
  
    // Singleton instance.
    private static RuntimeParameters _instance = initInstance();
  
    // Distinguished user-chosen name of this runtime instance.
    private String  instanceName;
  
	// Database configuration.
	private String  dbConnectionPoolName;
	private int     dbConnectionPoolSize;
	private String  dbUser;
	private String  dbPassword;
	private String  jdbcURL;
	private int     dbMeterMinutes;
	
	// Allow test query parameters to be used.
	private boolean allowTestQueryParms;
		
	// The slf4j/logback target directory and file.
	private String  logDirectory;
	private String  logFile;
	
	/* ********************************************************************** */
	/*                              Constructors                              */
	/* ********************************************************************** */
	/** This is where the work happens--either we can successfully create the
	 * singleton object or we throw an exception which should abort service
	 * initialization.  If an object is created, then all required input 
	 * parameters have been set in a syntactically valid way.
	 * 
	 * @throws TapisRuntimeException on error
	 */
	private RuntimeParameters()
	 throws TapisRuntimeException
	{
	  // --------------------- Get Input Parameters ---------------------
	  // Get the input parameter values from resource file and environment.
	  TapisInput tapisInput = new TapisInput(TapisConstants.SERVICE_NAME_SAMPLE);
	  Properties inputProperties = null;
	  try {inputProperties = tapisInput.getInputParameters();}
	  catch (TapisException e) {
	    // Very bad news.
	    String msg = MsgUtils.getMsg("TAPIS_SERVICE_INITIALIZATION_FAILED",
	                                 TapisConstants.SERVICE_NAME_SAMPLE,
	                                 e.getMessage());
	    _log.error(msg, e);
	    throw new TapisRuntimeException(msg, e);
	  }

	  // --------------------- Non-Configurable Parameters --------------
	  // We decide the pool name.
	  setDbConnectionPoolName(TapisConstants.SERVICE_NAME_SAMPLE + "Pool");
    
	  // --------------------- General Parameters -----------------------
	  // The name of this instance of the sample library that has meaning to
	  // humans, distinguishes this instance of the job service, and is 
	  // short enough to use to name runtime artifacts.
	  String parm = inputProperties.getProperty(EnvVar.TAPIS_INSTANCE_NAME.getEnvName());
	  if (StringUtils.isBlank(parm)) {
	      // Default to some string that's not too long and somewhat unique.
	      // We check the current value to avoid reassigning on reload.  The
	      // integer suffix can add up to 10 characters to the string.
	      if (getInstanceName() == null)
	          setInstanceName(TapisConstants.SERVICE_NAME_SAMPLE + 
                              Math.abs(new Random(System.currentTimeMillis()).nextInt()));
	  } 
	  else {
	      // Validate string length.
	      if (parm.length() > MAX_INSTANCE_NAME_LEN) {
	          String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                           TapisConstants.SERVICE_NAME_SAMPLE,
                                           "instanceName",
                                           "Instance name exceeds " + MAX_INSTANCE_NAME_LEN + "characters: " + parm);
	          _log.error(msg);
	          throw new TapisRuntimeException(msg);
      }
      if (!StringUtils.isAlphanumeric(parm)) {
              String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                           TapisConstants.SERVICE_NAME_SAMPLE,
                                           "instanceName",
                                           "Instance name contains non-alphanumeric characters: " + parm);
              _log.error(msg);
              throw new TapisRuntimeException(msg);
      }
    }
	
    // Logging level of the Maverick libary code
    parm = inputProperties.getProperty(EnvVar.TAPIS_LOG_DIRECTORY.getEnvName());
    if (!StringUtils.isBlank(parm)) setLogDirectory(parm);
                 
    // Logging level of the Maverick libary code
    parm = inputProperties.getProperty(EnvVar.TAPIS_LOG_FILE.getEnvName());
    if (!StringUtils.isBlank(parm)) setLogFile(parm);
                 
    // Optional test query parameter switch.
    parm = inputProperties.getProperty(EnvVar.TAPIS_ENVONLY_ALLOW_TEST_QUERY_PARMS.getEnvName());
    if (StringUtils.isBlank(parm)) setAllowTestQueryParms(false);
      else {
        try {setAllowTestQueryParms(Boolean.valueOf(parm));}
          catch (Exception e) {
            // Stop on bad input.
            String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                         TapisConstants.SERVICE_NAME_SAMPLE,
                                         "allowTestQueryParms",
                                         e.getMessage());
            _log.error(msg, e);
            throw new TapisRuntimeException(msg, e);
          }
      }
    
	  // --------------------- DB Parameters ----------------------------
    // User does not have to provide a pool size.
    parm = inputProperties.getProperty(EnvVar.TAPIS_DB_CONNECTION_POOL_SIZE.getEnvName());
    if (StringUtils.isBlank(parm)) setDbConnectionPoolSize(CONNECTION_POOL_SIZE);
      else {
        try {setDbConnectionPoolSize(Integer.valueOf(parm));}
          catch (Exception e) {
            // Stop on bad input.
            String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                         TapisConstants.SERVICE_NAME_SAMPLE,
                                         "dbConnectionPoolSize",
                                         e.getMessage());
            _log.error(msg, e);
            throw new TapisRuntimeException(msg, e);
          }
      }
    
    // DB user is required.
    parm = inputProperties.getProperty(EnvVar.TAPIS_DB_USER.getEnvName());
    if (StringUtils.isBlank(parm)) {
      // Stop on bad input.
      String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_MISSING",
                                   TapisConstants.SERVICE_NAME_SAMPLE,
                                   "dbUser");
      _log.error(msg);
      throw new TapisRuntimeException(msg);
    }
    setDbUser(parm);

    // DB user password is required.
    parm = inputProperties.getProperty(EnvVar.TAPIS_DB_PASSWORD.getEnvName());
    if (StringUtils.isBlank(parm)) {
      // Stop on bad input.
      String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_MISSING",
                                   TapisConstants.SERVICE_NAME_SAMPLE,
                                   "dbPassword");
      _log.error(msg);
      throw new TapisRuntimeException(msg);
    }
    setDbPassword(parm);
    
    // JDBC url is required.
    parm = inputProperties.getProperty(EnvVar.TAPIS_DB_JDBC_URL.getEnvName());
    if (StringUtils.isBlank(parm)) {
      // Stop on bad input.
      String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_MISSING",
                                   TapisConstants.SERVICE_NAME_SAMPLE,
                                   "jdbcUrl");
      _log.error(msg);
      throw new TapisRuntimeException(msg);
    }
    setJdbcURL(parm);

    // Specify zero or less minutes to turn off database metering.
    parm = inputProperties.getProperty(EnvVar.TAPIS_DB_METER_MINUTES.getEnvName());
    if (StringUtils.isBlank(parm)) setDbMeterMinutes(DEFAULT_DB_METER_INTERVAL_MINUTES);
      else {
        try {setDbConnectionPoolSize(Integer.valueOf(parm));}
          catch (Exception e) {
            // Stop on bad input.
            String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                         TapisConstants.SERVICE_NAME_SAMPLE,
                                         "dbMeterMinutes",
                                         e.getMessage());
            _log.error(msg, e);
            throw new TapisRuntimeException(msg, e);
          }
      }
	}
	 /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
	
	/* ---------------------------------------------------------------------- */
	/* initInstance:                                                          */
	/* ---------------------------------------------------------------------- */
	/** Initialize the singleton instance of this class.
	 * 
	 * @return the non-null singleton instance of this class
	 */
	private static synchronized RuntimeParameters initInstance()
	{
		if (_instance == null) _instance = new RuntimeParameters();
		return _instance;
	}
	
    /* ********************************************************************** */
	/*                             Public Methods                             */
	/* ********************************************************************** */
	/* ---------------------------------------------------------------------- */
	/* reload:                                                                */
	/* ---------------------------------------------------------------------- */
	/** Reload the parameters from scratch.  Should not be called too often,
	 * but does allow updates to parameter files and environment variables
	 * to be recognized.  
	 * 
	 * Note that concurrent calls to getInstance() will either return the 
	 * new or old parameters object, but whichever is returned it will be
	 * consistent.  Calls to specific parameter methods will also be 
	 * consistent, but the instance on which they are called may be stale
	 * if it was acquired before the last reload operation.  
	 * 
	 * @return a new instance of the runtime parameters
	 */
	public static synchronized RuntimeParameters reload()
	{
	  _instance = new RuntimeParameters();
	  return _instance;
	}
	
	/* ---------------------------------------------------------------------- */
	/* getLogSecurityInfo:                                                    */
	/* ---------------------------------------------------------------------- */
	/** Go directly to the environment to get the latest security info logging
	 * value.  This effectively disregards any setting the appears in a 
	 * properties file or on the JVM command line.
	 * 
	 * @return the current environment variable setting 
	 */
	public static boolean getLogSecurityInfo()
	{
	    // Always return the latest environment value.
	    return TapisEnv.getLogSecurityInfo();
	}
  
    /* ********************************************************************** */
    /*                               Accessors                                */
    /* ********************************************************************** */
	public static RuntimeParameters getInstance() {
		return _instance;
	}

	
	public String getDbConnectionPoolName() {
		return dbConnectionPoolName;
	}

	private void setDbConnectionPoolName(String dbConnectionPoolName) {
		this.dbConnectionPoolName = dbConnectionPoolName;
	}

	public int getDbConnectionPoolSize() {
		return dbConnectionPoolSize;
	}

	private void setDbConnectionPoolSize(int dbConnectionPoolSize) {
		this.dbConnectionPoolSize = dbConnectionPoolSize;
	}

	public String getDbUser() {
		return dbUser;
	}

	private void setDbUser(String dbUser) {
		this.dbUser = dbUser;
	}

	public String getDbPassword() {
		return dbPassword;
	}

	private void setDbPassword(String dbPassword) {
		this.dbPassword = dbPassword;
	}

	public String getJdbcURL() {
		return jdbcURL;
	}

	private void setJdbcURL(String jdbcURL) {
		this.jdbcURL = jdbcURL;
	}

	public String getInstanceName() {
	    return instanceName;
	}

	private void setInstanceName(String name) {
	    this.instanceName = name;
	}

	public static TapisUUID getId() {
	    return id;
	}

	public boolean isAllowTestQueryParms() {
	    return allowTestQueryParms;
	}

	private void setAllowTestQueryParms(boolean allowTestQueryParms) {
	    this.allowTestQueryParms = allowTestQueryParms;
	}

	public int getDbMeterMinutes() {
	    return dbMeterMinutes;
	}

	private void setDbMeterMinutes(int dbMeterMinutes) {
	    this.dbMeterMinutes = dbMeterMinutes;
	}

    public String getLogDirectory() {
        return logDirectory;
    }

    public void setLogDirectory(String logDirectory) {
        this.logDirectory = logDirectory;
    }

    public String getLogFile() {
        return logFile;
    }

    public void setLogFile(String logFile) {
        this.logFile = logFile;
    }
}

		

