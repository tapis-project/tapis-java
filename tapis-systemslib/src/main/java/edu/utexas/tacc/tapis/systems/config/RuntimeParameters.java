package edu.utexas.tacc.tapis.systems.config;

import java.text.NumberFormat;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.parameters.TapisEnv;
import edu.utexas.tacc.tapis.shared.parameters.TapisEnv.EnvVar;
import edu.utexas.tacc.tapis.shared.parameters.TapisInput;
import edu.utexas.tacc.tapis.shared.providers.email.EmailClientParameters;
import edu.utexas.tacc.tapis.shared.providers.email.enumeration.EmailProviderType;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
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
 * @author rcardone
 */
public final class RuntimeParameters 
 implements EmailClientParameters
{
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
    
    // Email defaults.
    private static final String DEFAULT_EMAIL_PROVIDER = "LOG";
    private static final int    DEFAULT_EMAIL_PORT = 25;
    private static final String DEFAULT_EMAIL_FROM_NAME = "Tapis Systems Service";
    private static final String DEFAULT_EMAIL_FROM_ADDRESS = "no-reply@nowhere.com";
    
    // Support defaults.
    private static final String DEFAULT_SUPPORT_NAME = "Oracle of Delphi";
     
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

	// Service config
	private String servicePassword;
	private String serviceMasterTenant;

	// Service base URLs - tenants, Security Kernel
	private String tenantsSvcURL;
	private String skSvcURL;

	// Mail configuration.
	private EmailProviderType emailProviderType;
	private boolean emailAuth;
	private String  emailHost;
	private int     emailPort;
	private String  emailUser;
	private String  emailPassword;
	private String  emailFromName;
	private String  emailFromAddress;
	
	// Support.
	private String  supportName;
	private String  supportEmail;
	
	// Allow test query parameters to be used.
	private boolean allowTestHeaderParms;
	
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
	  TapisInput tapisInput = new TapisInput(TapisConstants.SERVICE_NAME_SYSTEMS);
	  Properties inputProperties = null;
	  try {inputProperties = tapisInput.getInputParameters();}
	  catch (TapisException e) {
	    // Very bad news.
	    String msg = MsgUtils.getMsg("TAPIS_SERVICE_INITIALIZATION_FAILED",
	                                 TapisConstants.SERVICE_NAME_SYSTEMS,
	                                 e.getMessage());
	    _log.error(msg, e);
	    throw new TapisRuntimeException(msg, e);
	  }

	  // --------------------- Non-Configurable Parameters --------------
	  // We decide the pool name.
	  setDbConnectionPoolName(TapisConstants.SERVICE_NAME_SYSTEMS + "Pool");
    
	  // --------------------- General Parameters -----------------------
	  // The name of this instance of the systems library that has meaning to
	  // humans, distinguishes this instance of the job service, and is 
	  // short enough to use to name runtime artifacts.
	  String parm = inputProperties.getProperty(EnvVar.TAPIS_INSTANCE_NAME.getEnvName());
	  if (StringUtils.isBlank(parm)) {
	      // Default to some string that's not too long and somewhat unique.
	      // We check the current value to avoid reassigning on reload.  The
	      // integer suffix can add up to 10 characters to the string.
	      if (getInstanceName() == null)
	          setInstanceName(TapisConstants.SERVICE_NAME_SYSTEMS +
                              Math.abs(new Random(System.currentTimeMillis()).nextInt()));
	  } 
	  else {
	      // Validate string length.
	      if (parm.length() > MAX_INSTANCE_NAME_LEN) {
	          String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                           TapisConstants.SERVICE_NAME_SYSTEMS,
                                           "instanceName",
                                           "Instance name exceeds " + MAX_INSTANCE_NAME_LEN + "characters: " + parm);
	          _log.error(msg);
	          throw new TapisRuntimeException(msg);
      }
      if (!StringUtils.isAlphanumeric(parm)) {
              String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                           TapisConstants.SERVICE_NAME_SYSTEMS,
                                           "instanceName",
                                           "Instance name contains non-alphanumeric characters: " + parm);
              _log.error(msg);
              throw new TapisRuntimeException(msg);
      }
    }
	
    // Location of log file
    parm = inputProperties.getProperty(EnvVar.TAPIS_LOG_DIRECTORY.getEnvName());
    if (!StringUtils.isBlank(parm)) setLogDirectory(parm);
    parm = inputProperties.getProperty(EnvVar.TAPIS_LOG_FILE.getEnvName());
    if (!StringUtils.isBlank(parm)) setLogFile(parm);
                 
    // Optional test header parameter switch.
    parm = inputProperties.getProperty(EnvVar.TAPIS_ENVONLY_ALLOW_TEST_HEADER_PARMS.getEnvName());
    if (StringUtils.isBlank(parm)) setAllowTestHeaderParms(false);
      else {
        try {setAllowTestHeaderParms(Boolean.parseBoolean(parm));}
          catch (Exception e) {
            // Stop on bad input.
            String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                         TapisConstants.SERVICE_NAME_SYSTEMS,
                                         "allowTestQueryParms",
                                         e.getMessage());
            _log.error(msg, e);
            throw new TapisRuntimeException(msg, e);
          }
      }

		// --------------------- Service config --------------------------------
		parm = inputProperties.getProperty(EnvVar.TAPIS_SERVICE_PASSWORD.getEnvName());
		if (!StringUtils.isBlank(parm)) setServicePassword(parm);

		parm = inputProperties.getProperty(EnvVar2.TAPIS_SVC_MASTER_TENANT.getEnvName());
		if (!StringUtils.isBlank(parm)) setServiceMasterTenant(parm);

		// --------------------- Base URLs for other services that this service requires ----------------------------
		// Tenants service base URL is required. Throw runtime exception if not found.
		// Security Kernel base URL is optional. Normally it is retrieved from the Tenants service.
		parm = inputProperties.getProperty(EnvVar.TAPIS_TENANT_SVC_BASEURL.getEnvName());
		if (StringUtils.isBlank(parm)) {
			String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_MISSING", TapisConstants.SERVICE_NAME_SYSTEMS, "tenantsSvcUrl");
			_log.error(msg);
			throw new TapisRuntimeException(msg);
		}
		setTenantsSvcURL(parm);

		parm = inputProperties.getProperty(EnvVar2.TAPIS_SVC_URL_SK.getEnvName());
		if (!StringUtils.isBlank(parm)) setSkSvcURL(parm);



	// --------------------- DB Parameters ----------------------------
    // User does not have to provide a pool size.
    parm = inputProperties.getProperty(EnvVar.TAPIS_DB_CONNECTION_POOL_SIZE.getEnvName());
    if (StringUtils.isBlank(parm)) setDbConnectionPoolSize(CONNECTION_POOL_SIZE);
      else {
        try {setDbConnectionPoolSize(Integer.parseInt(parm));}
          catch (Exception e) {
            // Stop on bad input.
            String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                         TapisConstants.SERVICE_NAME_SYSTEMS,
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
                                   TapisConstants.SERVICE_NAME_SYSTEMS,
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
                                   TapisConstants.SERVICE_NAME_SYSTEMS,
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
                                   TapisConstants.SERVICE_NAME_SYSTEMS,
                                   "jdbcUrl");
      _log.error(msg);
      throw new TapisRuntimeException(msg);
    }
    setJdbcURL(parm);

    // Specify zero or less minutes to turn off database metering.
    parm = inputProperties.getProperty(EnvVar.TAPIS_DB_METER_MINUTES.getEnvName());
    if (StringUtils.isBlank(parm)) setDbMeterMinutes(DEFAULT_DB_METER_INTERVAL_MINUTES);
      else {
        try {setDbConnectionPoolSize(Integer.parseInt(parm));}
          catch (Exception e) {
            // Stop on bad input.
            String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                         TapisConstants.SERVICE_NAME_SYSTEMS,
                                         "dbMeterMinutes",
                                         e.getMessage());
            _log.error(msg, e);
            throw new TapisRuntimeException(msg, e);
          }
      }
    
    // --------------------- Email Parameters -------------------------
    // Currently LOG or SMTP.
    parm = inputProperties.getProperty(EnvVar.TAPIS_MAIL_PROVIDER.getEnvName());
    if (StringUtils.isBlank(parm)) parm = DEFAULT_EMAIL_PROVIDER;
    try {setEmailProviderType(EmailProviderType.valueOf(parm));}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                         TapisConstants.SERVICE_NAME_SYSTEMS,
                                         "emalProviderType",
                                         e.getMessage());
            _log.error(msg, e);
            throw new TapisRuntimeException(msg, e);
        }
    
    // Is authentication required?
    parm = inputProperties.getProperty(EnvVar.TAPIS_SMTP_AUTH.getEnvName());
    if (StringUtils.isBlank(parm)) setEmailAuth(false);
      else {
          try {setEmailAuth(Boolean.parseBoolean(parm));}
              catch (Exception e) {
                  // Stop on bad input.
                  String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                               TapisConstants.SERVICE_NAME_SYSTEMS,
                                               "emailAuth",
                                               e.getMessage());
                  _log.error(msg, e);
                  throw new TapisRuntimeException(msg, e);
              }
      }
    
    // Get the email server host.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SMTP_HOST.getEnvName());
    if (!StringUtils.isBlank(parm)) setEmailHost(parm);
      else if (getEmailProviderType() == EmailProviderType.SMTP) {
          String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_MISSING",
                                       TapisConstants.SERVICE_NAME_SYSTEMS,
                                       "emailHost");
          _log.error(msg);
          throw new TapisRuntimeException(msg);
      }
        
    // Get the email server port.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SMTP_PORT.getEnvName());
    if (StringUtils.isBlank(parm)) setEmailPort(DEFAULT_EMAIL_PORT);
      else
        try {setEmailPort(Integer.parseInt(parm));}
          catch (Exception e) {
              // Stop on bad input.
              String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                           TapisConstants.SERVICE_NAME_SYSTEMS,
                                           "emailPort",
                                           e.getMessage());
              _log.error(msg, e);
              throw new TapisRuntimeException(msg, e);
          }

    // Get the email server host.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SMTP_USER.getEnvName());
    if (!StringUtils.isBlank(parm)) setEmailUser(parm);
      else if (isEmailAuth()) {
          String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_MISSING",
                                       TapisConstants.SERVICE_NAME_SYSTEMS,
                                       "emailUser");
          _log.error(msg);
          throw new TapisRuntimeException(msg);
      }
        
    // Get the email server host.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SMTP_PASSWORD.getEnvName());
    if (!StringUtils.isBlank(parm)) setEmailPassword(parm);
      else if (isEmailAuth()) {
        String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_MISSING",
                                     TapisConstants.SERVICE_NAME_SYSTEMS,
                                     "emailPassword");
        _log.error(msg);
        throw new TapisRuntimeException(msg);
      }
        
    // Get the email server host.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SMTP_FROM_NAME.getEnvName());
    if (!StringUtils.isBlank(parm)) setEmailFromName(parm);
      else setEmailFromName(DEFAULT_EMAIL_FROM_NAME);
        
    // Get the email server host.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SMTP_FROM_ADDRESS.getEnvName());
    if (!StringUtils.isBlank(parm)) setEmailFromAddress(parm);
      else setEmailFromAddress(DEFAULT_EMAIL_FROM_ADDRESS);
    
    // --------------------- Support Parameters -----------------------
    // Chose a name for support or one will be chosen.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SUPPORT_NAME.getEnvName());
    if (!StringUtils.isBlank(parm)) setSupportName(parm);
     else setSupportName(DEFAULT_SUPPORT_NAME);
    
    // Empty support email means no support emails will be sent.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SUPPORT_EMAIL.getEnvName());
    if (!StringUtils.isBlank(parm)) setSupportEmail(parm);
  }
	
    /* ---------------------------------------------------------------------- */
    /* getRuntimeInfo:                                                        */
    /* ---------------------------------------------------------------------- */
	/** Augment the buffer with printable text based mostly on the parameters
	 * managed by this class but also OS and JVM information.  The intent is 
	 * that the various job programs and utilities that rely on this class can
	 * print their configuration parameters, including those from this class, 
	 * when they start up.  
	 * 
	 * @param buf
	 */
	public void getRuntimeInfo(StringBuilder buf)
	{
		buf.append("\n------- Logging -----------------------------------");
		buf.append("\ntapis.log.directory: ");
		buf.append(this.getLogDirectory());
		buf.append("\ntapis.log.file: ");
		buf.append(this.getLogFile());

		buf.append("\n------- Network -----------------------------------");
		buf.append("\nHost Addresses: ");
		buf.append(getNetworkAddresses());

		buf.append("\n------- DB Configuration --------------------------");
		buf.append("\ntapis.db.jdbc.url: ");
		buf.append(this.getJdbcURL());
		buf.append("\ntapis.db.user: ");
		buf.append(this.getDbUser());
		buf.append("\ntapis.db.connection.pool.size: ");
		buf.append(this.getDbConnectionPoolSize());
		buf.append("\ntapis.db.meter.minutes: ");
		buf.append(this.getDbMeterMinutes());

		buf.append("\n------- Base Service URLs --------------------------");
		buf.append("\ntapis.svc.tenants.url: ");
		buf.append(tenantsSvcURL);
		buf.append("\ntapis.svc.sk.url: ");
		buf.append(skSvcURL);

		buf.append("\n------- Email Configuration -----------------------");
		buf.append("\ntapis.mail.provider: ");
		buf.append(this.getEmailProviderType().name());
		buf.append("\ntapis.smtp.auth: ");
		buf.append(this.isEmailAuth());
		buf.append("\ntapis.smtp.host: ");
		buf.append(this.getEmailHost());
		buf.append("\ntapis.smtp.port: ");
		buf.append(this.getEmailPort());
		buf.append("\ntapis.smtp.user: ");
		buf.append(this.getEmailUser());
		buf.append("\ntapis.smtp.from.name: ");
		buf.append(this.getEmailFromName());
		buf.append("\ntapis.smtp.from.address: ");
		buf.append(this.getEmailFromAddress());

		buf.append("\n------- Support Configuration ---------------------");
		buf.append("\ntapis.support.name: ");
		buf.append(this.getSupportName());
		buf.append("\ntapis.support.email: ");
		buf.append(this.getSupportEmail());

		buf.append("\n------- EnvOnly Configuration ---------------------");
		buf.append("\ntapis.envonly.log.security.info: ");
		buf.append(RuntimeParameters.getLogSecurityInfo());
		buf.append("\ntapis.envonly.allow.test.header.parms: ");
		buf.append(this.isAllowTestHeaderParms());
		buf.append("\ntapis.envonly.jwt.optional: ");
		buf.append(TapisEnv.getBoolean(EnvVar.TAPIS_ENVONLY_JWT_OPTIONAL));
		buf.append("\ntapis.envonly.skip.jwt.verify: ");
		buf.append(TapisEnv.getBoolean(EnvVar.TAPIS_ENVONLY_SKIP_JWT_VERIFY));

		buf.append("\n------- Java Configuration ------------------------");
		buf.append("\njava.version: ");
		buf.append(System.getProperty("java.version"));
		buf.append("\njava.vendor: ");
		buf.append(System.getProperty("java.vendor"));
		buf.append("\njava.vm.version: ");
		buf.append(System.getProperty("java.vm.version"));
		buf.append("\njava.vm.vendor: ");
		buf.append(System.getProperty("java.vm.vendor"));
		buf.append("\njava.vm.name: ");
		buf.append(System.getProperty("java.vm.name"));
		buf.append("\nos.name: ");
		buf.append(System.getProperty("os.name"));
		buf.append("\nos.arch: ");
		buf.append(System.getProperty("os.arch"));
		buf.append("\nos.version: ");
		buf.append(System.getProperty("os.version"));
		buf.append("\nuser.name: ");
		buf.append(System.getProperty("user.name"));
		buf.append("\nuser.home: ");
		buf.append(System.getProperty("user.home"));
		buf.append("\nuser.dir: ");
		buf.append(System.getProperty("user.dir"));

		buf.append("\n------- JVM Runtime Values ------------------------");
		NumberFormat formatter = NumberFormat.getIntegerInstance();
		buf.append("\navailableProcessors: ");
		buf.append(formatter.format(Runtime.getRuntime().availableProcessors()));
		buf.append("\nmaxMemory: ");
		buf.append(formatter.format(Runtime.getRuntime().maxMemory()));
		buf.append("\ntotalMemory: ");
		buf.append(formatter.format(Runtime.getRuntime().totalMemory()));
		buf.append("\nfreeMemory: ");
		buf.append(formatter.format(Runtime.getRuntime().freeMemory()));
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
	
    /* ---------------------------------------------------------------------- */
    /* getNetworkAddresses:                                                   */
    /* ---------------------------------------------------------------------- */
	/** Best effort attempt to get the network addresses of this host for 
	 * logging purposes.
	 * 
	 * @return the comma separated string of IP addresses or null
	 */
    private String getNetworkAddresses()
    {
        // Comma separated result string.
        String addresses = null;
        
        // Best effort attempt to get this host's ip addresses.
        try {
            List<String> list = TapisUtils.getIpAddressesFromNetInterface();
            if (!list.isEmpty()) { 
                String[] array = new String[list.size()];
                array = list.toArray(array);
                addresses = String.join(", ", array);
            }
        }
        catch (Exception e) {/* ignore exceptions */}
        
        // Can be null.
        return addresses;
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

	public String getServiceMasterTenant() { return serviceMasterTenant; }
	private void setServiceMasterTenant(String t) { serviceMasterTenant = t; }

	public String getServicePassword() { return servicePassword; }
	private void setServicePassword(String p) {servicePassword = p; }

	public String getTenantsSvcURL() { return tenantsSvcURL; }
	private void setTenantsSvcURL(String url) {tenantsSvcURL = url; }

	public String getSkSvcURL() { return skSvcURL; }
	private void setSkSvcURL(String url) {skSvcURL = url; }

	public String getInstanceName() {
	    return instanceName;
	}

	private void setInstanceName(String name) {
	    this.instanceName = name;
	}

	public static TapisUUID getId() {
	    return id;
	}

	public boolean isAllowTestHeaderParms() {
	    return allowTestHeaderParms;
	}

	private void setAllowTestHeaderParms(boolean allowTestHeaderParms) {
	    this.allowTestHeaderParms = allowTestHeaderParms;
	}

	public int getDbMeterMinutes() {
	    return dbMeterMinutes;
	}

	private void setDbMeterMinutes(int dbMeterMinutes) {
	    this.dbMeterMinutes = dbMeterMinutes;
	}

    public EmailProviderType getEmailProviderType() {
        return emailProviderType;
    }

    public void setEmailProviderType(EmailProviderType emailProviderType) {
        this.emailProviderType = emailProviderType;
    }

    public boolean isEmailAuth() {
        return emailAuth;
    }

    public void setEmailAuth(boolean emailAuth) {
        this.emailAuth = emailAuth;
    }

    public String getEmailHost() {
        return emailHost;
    }

    public void setEmailHost(String emailHost) {
        this.emailHost = emailHost;
    }

    public int getEmailPort() {
        return emailPort;
    }

    public void setEmailPort(int emailPort) {
        this.emailPort = emailPort;
    }

    public String getEmailUser() {
        return emailUser;
    }

    public void setEmailUser(String emailUser) {
        this.emailUser = emailUser;
    }

    public String getEmailPassword() {
        return emailPassword;
    }

    public void setEmailPassword(String emailPassword) {
        this.emailPassword = emailPassword;
    }

    public String getEmailFromName() {
        return emailFromName;
    }

    public void setEmailFromName(String emailFromName) {
        this.emailFromName = emailFromName;
    }

    public String getEmailFromAddress() {
        return emailFromAddress;
    }

    public void setEmailFromAddress(String emailFromAddress) {
        this.emailFromAddress = emailFromAddress;
    }

    public String getSupportName() {
        return supportName;
    }

    public void setSupportName(String supportName) {
        this.supportName = supportName;
    }

    public String getSupportEmail() {
        return supportEmail;
    }

    public void setSupportEmail(String supportEmail) {
        this.supportEmail = supportEmail;
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


    // TODO/TBD move this to shared TapisEnv?
    // TODO/TBD Remove sk url. Always look up from tenants svc
	private enum EnvVar2 {
		TAPIS_SVC_URL_SK("tapis.svc.url.sk"),
		TAPIS_SVC_MASTER_TENANT("tapis.svc.master.tenant");

		private final String _envName;

		EnvVar2(String envName) {
			_envName = envName;
		}

		public String getEnvName() {
			return this._envName;
		}
	}
}
