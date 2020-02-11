package edu.utexas.tacc.tapis.security.config;

import java.text.NumberFormat;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.secrets.IVaultManagerParms;
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
 implements EmailClientParameters, IVaultManagerParms
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
    private static final String DEFAULT_EMAIL_FROM_NAME = "Tapis Security Service";
    private static final String DEFAULT_EMAIL_FROM_ADDRESS = "no-reply@nowhere.com";
    
    // Support defaults.
    private static final String DEFAULT_SUPPORT_NAME = "Oracle of Delphi";
    
    // Vault defaults.
    private static final int DEFAULT_VAULT_OPEN_TIMEOUT_SECS = 20;
    private static final int DEFAULT_VAULT_READ_TIMEOUT_SECS = 20;
     
    // Vault token renewal defaults.
    private static final int DEFAULT_VAULT_TOKEN_SECONDS = 28800; // 4 hours
    private static final int MIN_VAULT_TOKEN_SECONDS = 60;        // 1 minute
    private static final int DEFAULT_VAULT_TOKEN_THRESHOLD = 50;  // percent 
    private static final int MIN_VAULT_TOKEN_THRESHOLD = 20;      // percent 
    private static final int MAX_VAULT_TOKEN_THRESHOLD = 80;      // percent
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // Globally unique id that identifies this JVM instance.
    private static final TapisUUID id = new TapisUUID(UUIDType.TAPIS_SECURITY_KERNEL); 
  
    // Singleton instance.
    private static RuntimeParameters _instance = initInstance();
  
    // Distinguished user-chosen name of this runtime instance.
    private String  instanceName;
    
    // Tenant service location.
    private String  tenantBaseUrl;
  
	// Database configuration.
	private String  dbConnectionPoolName;
	private int     dbConnectionPoolSize;
	private String  dbUser;
	private String  dbPassword;
	private String  jdbcURL;
	private int     dbMeterMinutes;
	
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
	
	// Vault parameters.
	private boolean vaultDisabled;       // disable vault processing
	private String  vaultAddress;        // vault server address
	private String  vaultRoleId;         // approle role id assigned to SK for logon
	private String  vaultSecretId;       // approle secret id for logon
	private int     vaultOpenTimeout;    // connection timeout in seconds
	private int     vaultReadTimeout;    // read response timeout in seconds
	private boolean vaultSslVerify;      // whether to use http or https
	private String  vaultSslCertFile;    // certificate file containing vault's public key
	private String  vaultSkKeyPemFile;   // PEM file containing SK's private key 
	private int     vaultRenewSeconds;   // expiration time in seconds of SK token
	private int     vaultRenewThreshold; // point at which token renewal begins,
	                                     //   expressed as percent of expiration time
	
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
	  // Announce parameter initialization.
	  _log.info(MsgUtils.getMsg("TAPIS_INITIALIZING_SERVICE", TapisConstants.SERVICE_NAME_SECURITY));
	    
	  // --------------------- Get Input Parameters ---------------------
	  // Get the input parameter values from resource file and environment.
	  TapisInput tapisInput = new TapisInput(TapisConstants.SERVICE_NAME_SECURITY);
	  Properties inputProperties = null;
	  try {inputProperties = tapisInput.getInputParameters();}
	  catch (TapisException e) {
	    // Very bad news.
	    String msg = MsgUtils.getMsg("TAPIS_SERVICE_INITIALIZATION_FAILED",
	                                 TapisConstants.SERVICE_NAME_SECURITY,
	                                 e.getMessage());
	    _log.error(msg, e);
	    throw new TapisRuntimeException(msg, e);
	  }

	  // --------------------- Non-Configurable Parameters --------------
	  // We decide the pool name.
	  setDbConnectionPoolName(TapisConstants.SERVICE_NAME_SECURITY + "Pool");
    
	  // --------------------- General Parameters -----------------------
	  // The name of this instance of the security library that has meaning to
	  // humans, distinguishes this instance of the job service, and is 
	  // short enough to use to name runtime artifacts.
	  String parm = inputProperties.getProperty(EnvVar.TAPIS_INSTANCE_NAME.getEnvName());
	  if (StringUtils.isBlank(parm)) {
	      // Default to some string that's not too long and somewhat unique.
	      // We check the current value to avoid reassigning on reload.  The
	      // integer suffix can add up to 10 characters to the string.
	      if (getInstanceName() == null)
	          setInstanceName(TapisConstants.SERVICE_NAME_SECURITY + 
                              Math.abs(new Random(System.currentTimeMillis()).nextInt()));
	  } 
	  else {
	      // Validate string length.
	      if (parm.length() > MAX_INSTANCE_NAME_LEN) {
	          String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                           TapisConstants.SERVICE_NAME_SECURITY,
                                           "instanceName",
                                           "Instance name exceeds " + MAX_INSTANCE_NAME_LEN + "characters: " + parm);
	          _log.error(msg);
	          throw new TapisRuntimeException(msg);
      }
      if (!StringUtils.isAlphanumeric(parm)) {
              String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                           TapisConstants.SERVICE_NAME_SECURITY,
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
                 
    // Optional test header parameter switch.
    parm = inputProperties.getProperty(EnvVar.TAPIS_ENVONLY_ALLOW_TEST_HEADER_PARMS.getEnvName());
    if (StringUtils.isBlank(parm)) setAllowTestHeaderParms(false);
      else {
        try {setAllowTestHeaderParms(Boolean.valueOf(parm));}
          catch (Exception e) {
            // Stop on bad input.
            String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                         TapisConstants.SERVICE_NAME_SECURITY,
                                         "allowTestQueryParms",
                                         e.getMessage());
            _log.error(msg, e);
            throw new TapisRuntimeException(msg, e);
          }
      }
    
    // --------------------- Tenant Parameters ------------------------
    // We need to know where the tenant service is locaated.
    parm = inputProperties.getProperty(EnvVar.TAPIS_TENANT_SVC_BASEURL.getEnvName());
    if (!StringUtils.isBlank(parm)) setTenantBaseUrl(parm);
    
	// --------------------- DB Parameters ----------------------------
    // User does not have to provide a pool size.
    parm = inputProperties.getProperty(EnvVar.TAPIS_DB_CONNECTION_POOL_SIZE.getEnvName());
    if (StringUtils.isBlank(parm)) setDbConnectionPoolSize(CONNECTION_POOL_SIZE);
      else {
        try {setDbConnectionPoolSize(Integer.valueOf(parm));}
          catch (Exception e) {
            // Stop on bad input.
            String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                         TapisConstants.SERVICE_NAME_SECURITY,
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
                                   TapisConstants.SERVICE_NAME_SECURITY,
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
                                   TapisConstants.SERVICE_NAME_SECURITY,
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
                                   TapisConstants.SERVICE_NAME_SECURITY,
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
                                         TapisConstants.SERVICE_NAME_SECURITY,
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
                                         TapisConstants.SERVICE_NAME_SECURITY,
                                         "emalProviderType",
                                         e.getMessage());
            _log.error(msg, e);
            throw new TapisRuntimeException(msg, e);
        }
    
    // Is authentication required?
    parm = inputProperties.getProperty(EnvVar.TAPIS_SMTP_AUTH.getEnvName());
    if (StringUtils.isBlank(parm)) setEmailAuth(false);
      else {
          try {setEmailAuth(Boolean.valueOf(parm));}
              catch (Exception e) {
                  // Stop on bad input.
                  String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                               TapisConstants.SERVICE_NAME_SECURITY,
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
                                       TapisConstants.SERVICE_NAME_SECURITY,
                                       "emailHost");
          _log.error(msg);
          throw new TapisRuntimeException(msg);
      }
        
    // Get the email server port.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SMTP_PORT.getEnvName());
    if (StringUtils.isBlank(parm)) setEmailPort(DEFAULT_EMAIL_PORT);
      else
        try {setEmailPort(Integer.valueOf(parm));}
          catch (Exception e) {
              // Stop on bad input.
              String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                           TapisConstants.SERVICE_NAME_SECURITY,
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
                                       TapisConstants.SERVICE_NAME_SECURITY,
                                       "emailUser");
          _log.error(msg);
          throw new TapisRuntimeException(msg);
      }
        
    // Get the email server host.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SMTP_PASSWORD.getEnvName());
    if (!StringUtils.isBlank(parm)) setEmailPassword(parm);
      else if (isEmailAuth()) {
        String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_MISSING",
                                     TapisConstants.SERVICE_NAME_SECURITY,
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
    
    // --------------------- Vault Parameters -------------------------
    // Determine whether the secrets subsystem is disabled.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SK_VAULT_DISABLE.getEnvName());
    if (StringUtils.isBlank(parm)) setVaultDisabled(false);
      else {
        try {setVaultDisabled(Boolean.valueOf(parm));}
          catch (Exception e) {
            // Stop on bad input.
            String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                         TapisConstants.SERVICE_NAME_SECURITY,
                                         "vaultDisabled",
                                         e.getMessage());
            _log.error(msg, e);
            throw new TapisRuntimeException(msg, e);
          }
      }

    // Make sure we have the address of the vault server (ex: http://myhost:8200)
    // unless the secrets subsystem is disabled.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SK_VAULT_ADDRESS.getEnvName());
    if (!StringUtils.isBlank(parm)) setVaultAddress(parm);
      else if (!vaultDisabled) {
          // A vault server address is required.
          String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                       TapisConstants.SERVICE_NAME_SECURITY,
                                       "vaultAddress",
                                       MsgUtils.getMsg("SK_VAULT_MISSING_VAULT_PARM", 
                                          EnvVar.TAPIS_SK_VAULT_ADDRESS.getEnvName()));
          _log.error(msg);
          throw new TapisRuntimeException(msg);
        }
        
    // Make sure we have the security kernel's role id unless the secrets subsystem is disabled.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SK_VAULT_ROLE_ID.getEnvName());
    if (!StringUtils.isBlank(parm)) setVaultRoleId(parm);
      else if (!vaultDisabled) {
          // A vault server address is required.
          String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                       TapisConstants.SERVICE_NAME_SECURITY,
                                       "vaultRoleId",
                                       MsgUtils.getMsg("SK_VAULT_MISSING_VAULT_PARM",
                                          EnvVar.TAPIS_SK_VAULT_ROLE_ID.getEnvName()));
          _log.error(msg);
          throw new TapisRuntimeException(msg);
        }
      
    // Make sure we have the security kernel's secret id unless the secrets subsystem is disabled.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SK_VAULT_SECRET_ID.getEnvName());
    if (!StringUtils.isBlank(parm)) setVaultSecretId(parm);
      else if (!vaultDisabled) {
          // A vault server address is required.
          String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                       TapisConstants.SERVICE_NAME_SECURITY,
                                       "vaultSecretId",
                                       MsgUtils.getMsg("SK_VAULT_MISSING_VAULT_PARM",
                                          EnvVar.TAPIS_SK_VAULT_SECRET_ID.getEnvName()));
          _log.error(msg);
          throw new TapisRuntimeException(msg);
        }
    
    // Get the open timeout.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SK_VAULT_OPEN_TIMEOUT.getEnvName());
    if (StringUtils.isBlank(parm)) setVaultOpenTimeout(DEFAULT_VAULT_OPEN_TIMEOUT_SECS);
      else
        try {setVaultOpenTimeout(Integer.valueOf(parm));}
          catch (Exception e) {
              // Stop on bad input.
              String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                           TapisConstants.SERVICE_NAME_SECURITY,
                                           "vaultOpenTimeout",
                                           e.getMessage());
              _log.error(msg, e);
              throw new TapisRuntimeException(msg, e);
          }
    
    // Get the read timeout.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SK_VAULT_READ_TIMEOUT.getEnvName());
    if (StringUtils.isBlank(parm)) setVaultReadTimeout(DEFAULT_VAULT_READ_TIMEOUT_SECS);
      else
        try {setVaultReadTimeout(Integer.valueOf(parm));}
          catch (Exception e) {
              // Stop on bad input.
              String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                           TapisConstants.SERVICE_NAME_SECURITY,
                                           "vaultReadTimeout",
                                           e.getMessage());
              _log.error(msg, e);
              throw new TapisRuntimeException(msg, e);
          }
    
    // Determine whether SSL certificate authentication is to be used.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SK_VAULT_SSL_VERIFY.getEnvName());
    if (StringUtils.isBlank(parm)) setVaultSslVerify(false);
      else {
        try {setVaultSslVerify(Boolean.valueOf(parm));}
          catch (Exception e) {
            // Stop on bad input.
            String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                         TapisConstants.SERVICE_NAME_SECURITY,
                                         "vaultSslVerity",
                                         e.getMessage());
            _log.error(msg, e);
            throw new TapisRuntimeException(msg, e);
          }
      }

    // Make sure we have a certificate file name unless SSL verify is off.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SK_VAULT_SSL_CERT_FILE.getEnvName());
    if (!StringUtils.isBlank(parm)) setVaultSslCertFile(parm);
      else if (!vaultDisabled && vaultSslVerify) {
          // A vault server address is required.
          String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                       TapisConstants.SERVICE_NAME_SECURITY,
                                       "vaultSslCertFile",
                                       MsgUtils.getMsg("SK_VAULT_MISSING_VAULT_PARM",
                                          EnvVar.TAPIS_SK_VAULT_SSL_CERT_FILE.getEnvName()));
          _log.error(msg);
          throw new TapisRuntimeException(msg);
        }
    
    // Assign the expiration time on tokens we acquire. This duration can be no longer
    // than what's configured in the sk-role.json file.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SK_VAULT_TOKEN_RENEWAL_SECONDS.getEnvName());
    if (StringUtils.isBlank(parm)) setVaultRenewSeconds(DEFAULT_VAULT_TOKEN_SECONDS);
      else 
          try {
              int expiry = Integer.valueOf(parm);
              if (expiry < MIN_VAULT_TOKEN_SECONDS) {
                  String msg = MsgUtils.getMsg("TAPIS_PARAMETER_LESS_THAN_MIN",
                                               EnvVar.TAPIS_SK_VAULT_TOKEN_RENEWAL_SECONDS.getEnvName(),
                                               expiry,
                                               MIN_VAULT_TOKEN_SECONDS);
                   throw new IllegalArgumentException(msg);
              }

              // We're good.
              setVaultRenewSeconds(expiry);
          }
          catch (Exception e) {
              // Stop on bad input.
              String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                           TapisConstants.SERVICE_NAME_SECURITY,
                                           "vaultTokenSeconds",
                                           e.getMessage());
              _log.error(msg, e);
              throw new TapisRuntimeException(msg, e);
          }
  
    // Get the threshold after which we try to renew our token. The threshold is expressed
    // as a percentage of the total ttl of the token.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SK_VAULT_TOKEN_RENEWAL_THRESHOLD.getEnvName());
    if (StringUtils.isBlank(parm)) setVaultRenewThreshold(DEFAULT_VAULT_TOKEN_THRESHOLD);
      else 
          try {
              // Check that the parameter is an integer in range.
              int threshold = Integer.valueOf(parm);
              if (threshold < MIN_VAULT_TOKEN_THRESHOLD || 
                  threshold > MAX_VAULT_TOKEN_THRESHOLD) {
                  String msg = MsgUtils.getMsg("TAPIS_PARAMETER_OUT_OF_RANGE",
                                               EnvVar.TAPIS_SK_VAULT_TOKEN_RENEWAL_THRESHOLD.getEnvName(),
                                               threshold,
                                               MIN_VAULT_TOKEN_THRESHOLD,
                                               MAX_VAULT_TOKEN_THRESHOLD);
                   throw new IllegalArgumentException(msg);
              }
                  
              // We're good.
              setVaultRenewThreshold(threshold);
          }
          catch (Exception e) {
              // Stop on bad input.
             String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
                                          TapisConstants.SERVICE_NAME_SECURITY,
                                          "vaultTokenThreshold",
                                          e.getMessage());
              _log.error(msg, e);
              throw new TapisRuntimeException(msg, e);
          }
    
    // Determine if a private key file has been specified.
    // TODO: we'll probably need to adjust this when mutual auth is actually used.
    parm = inputProperties.getProperty(EnvVar.TAPIS_SK_VAULT_SK_KEY_PEM_FILE.getEnvName());
    if (StringUtils.isBlank(parm)) setVaultSkKeyPemFile(parm);
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
        
        buf.append("\n------- Tenants -----------------------------------");
        buf.append("\ntapis.tenant.svc.baseurl: ");
	    
	    buf.append("\n------- DB Configuration --------------------------");
	    buf.append("\ntapis.db.jdbc.url: ");
	    buf.append(this.getJdbcURL());
	    buf.append("\ntapis.db.user: ");
	    buf.append(this.getDbUser());
	    buf.append("\ntapis.db.connection.pool.size: ");
	    buf.append(this.getDbConnectionPoolSize());
	    buf.append("\ntapis.db.meter.minutes: ");
	    buf.append(this.getDbMeterMinutes());
	    
        buf.append("\n------- Vault Configuration -----------------------");
        buf.append("\ntapis.sk.vault.disable: ");
        buf.append(this.isVaultDisabled());
        buf.append("\ntapis.sk.vault.address: ");
        buf.append(this.getVaultAddress());
        buf.append("\ntapis.sk.vault.roleid: ");
        buf.append(this.getVaultRoleId() == null ? null : "****");
        buf.append("\ntapis.sk.vault.secretid: ");
        buf.append(this.getVaultSecretId() == null ? null : "****");
        buf.append("\ntapis.sk.vault.open.timeout: ");
        buf.append(this.getVaultOpenTimeout());
        buf.append("\ntapis.sk.vault.read.timeout: ");
        buf.append(this.getVaultReadTimeout());
        buf.append("\ntapis.sk.vault.ssl.verify: ");
        buf.append(this.isVaultSslVerify());
        buf.append("\ntapis.sk.vault.ssl.cert: ");
        buf.append(this.getVaultSslCertFile());
        buf.append("\ntapis.sk.vault.sk.key.pem.file: ");
        buf.append(this.getVaultSkKeyPemFile());
        buf.append("\ntapis.sk.vault.token.seconds: ");
        buf.append(this.getVaultRenewSeconds());
        buf.append("\ntapis.sk.vault.token.renewal.threshold: ");
        buf.append(this.getVaultRenewThreshold());
        
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

    public boolean isVaultDisabled() {
        return vaultDisabled;
    }

    public void setVaultDisabled(boolean vaultDisabled) {
        this.vaultDisabled = vaultDisabled;
    }

    public String getVaultAddress() {
        return vaultAddress;
    }

    public void setVaultAddress(String vaultAddress) {
        this.vaultAddress = vaultAddress;
    }

    public String getVaultRoleId() {
        return vaultRoleId;
    }

    public void setVaultRoleId(String vaultRoleId) {
        this.vaultRoleId = vaultRoleId;
    }

    public String getVaultSecretId() {
        return vaultSecretId;
    }

    public void setVaultSecretId(String vaultSecretId) {
        this.vaultSecretId = vaultSecretId;
    }

    public int getVaultOpenTimeout() {
        return vaultOpenTimeout;
    }

    public void setVaultOpenTimeout(int vaultOpenTimeout) {
        this.vaultOpenTimeout = vaultOpenTimeout;
    }

    public int getVaultReadTimeout() {
        return vaultReadTimeout;
    }

    public void setVaultReadTimeout(int vaultReadTimeout) {
        this.vaultReadTimeout = vaultReadTimeout;
    }

    public boolean isVaultSslVerify() {
        return vaultSslVerify;
    }

    public void setVaultSslVerify(boolean vaultSslVerify) {
        this.vaultSslVerify = vaultSslVerify;
    }

    public String getVaultSslCertFile() {
        return vaultSslCertFile;
    }

    public void setVaultSslCertFile(String vaultSslCertFile) {
        this.vaultSslCertFile = vaultSslCertFile;
    }

    public String getVaultSkKeyPemFile() {
        return vaultSkKeyPemFile;
    }

    public void setVaultSkKeyPemFile(String vaultSkKeyPemFile) {
        this.vaultSkKeyPemFile = vaultSkKeyPemFile;
    }

    public int getVaultRenewSeconds() {
        return vaultRenewSeconds;
    }

    public void setVaultRenewSeconds(int vaultRenewSeconds) {
        this.vaultRenewSeconds = vaultRenewSeconds;
    }

    public int getVaultRenewThreshold() {
        return vaultRenewThreshold;
    }

    public void setVaultRenewThreshold(int vaultRenewThreshold) {
        this.vaultRenewThreshold = vaultRenewThreshold;
    }

    public String getTenantBaseUrl() {
        return tenantBaseUrl;
    }

    public void setTenantBaseUrl(String tenantBaseUrl) {
        this.tenantBaseUrl = tenantBaseUrl;
    }
}
