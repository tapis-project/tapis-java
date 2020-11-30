package edu.utexas.tacc.tapis.meta.config;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.parameters.TapisEnv;
import edu.utexas.tacc.tapis.shared.parameters.TapisInput;
import edu.utexas.tacc.tapis.shared.security.ServiceJWT;
import edu.utexas.tacc.tapis.shared.security.ServiceJWTParms;
import edu.utexas.tacc.tapis.shared.uuid.TapisUUID;
import edu.utexas.tacc.tapis.shared.uuid.UUIDType;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Properties;

public class RuntimeParameters {
  
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(RuntimeParameters.class);
  
  // Distinguished user-chosen name of this runtime instance.
  private String  instanceName;
  
  // The site in which this service is running.
  private String  siteId;
  
  // Globally unique id that identifies this JVM instance.
  private static final TapisUUID id = new TapisUUID(UUIDType.METADATA);
  
  // Singleton instance.
  private static RuntimeParameters _instance = initInstance();
  
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
  
  // service locations.
  // TODO: **************** Remove hardcoded paths and site ****************
  // TODO: FIX-FOR-ASSOCIATE-SITES 
  private String tenantBaseUrl = "https://dev.develop.tapis.io/";
  private String skSvcURL      = "https://dev.develop.tapis.io/v3";
  private String tokenBaseUrl  = "https://dev.develop.tapis.io/";
  private String site          = "tacc";
  private String metaToken;
  private ServiceJWT serviceJWT;
  
  // The slf4j/logback target directory and file.
  private String  logDirectory;
  private String  logFile;
  private String  coreServer = "http://restheart:8080/";
  
  
  // these need to move to shared library
  public static final String SERVICE_NAME_META  = "meta";
  public static final String SERVICE_USER_NAME  = "meta";
  public static final String SERVICE_TENANT_NAME = "master";
  
  
  private RuntimeParameters() throws TapisRuntimeException {
    TapisInput tapisInput = new TapisInput(RuntimeParameters.SERVICE_NAME_META);
    Properties inputProperties = null;
    try {inputProperties = tapisInput.getInputParameters();}
    catch (TapisException e) {
      // Very bad news.
      String msg = MsgUtils.getMsg("TAPIS_SERVICE_INITIALIZATION_FAILED",
          RuntimeParameters.SERVICE_NAME_META,
          e.getMessage());
      _log.error(msg, e);
      throw new TapisRuntimeException(msg, e);
    }
  
    // The site must always be provided.
    String parm = inputProperties.getProperty(TapisEnv.EnvVar.TAPIS_SITE_ID.getEnvName());
    if (StringUtils.isBlank(parm)) {
      String msg = MsgUtils.getMsg("TAPIS_SERVICE_PARM_INITIALIZATION_FAILED",
          RuntimeParameters.SERVICE_NAME_META,
          "siteId",
          "No siteId specified.");
      _log.error(msg);
      throw new TapisRuntimeException(msg);
    }
    setSiteId(parm);
  
    //----------------------   Input parameters   ----------------------
    // String parm = inputProperties.getProperty(TapisEnv.EnvVar.TAPIS_LOG_DIRECTORY.getEnvName());
    parm = System.getenv("tapis.meta.core.server");
    if (!StringUtils.isBlank(parm)) setCoreServer(parm);
  
    parm = inputProperties.getProperty("tapis.meta.log.directory");
    if (!StringUtils.isBlank(parm)) setLogDirectory(parm);
  
    parm = inputProperties.getProperty("tapis.meta.log.file");
    if (!StringUtils.isBlank(parm)) setLogFile(parm);
  
    parm = inputProperties.getProperty("tapis.meta.service.token");
    if (!StringUtils.isBlank(parm)) setMetaToken(parm);
  
    parm = System.getenv("tapis.meta.service.tenantBaseUrl");
    if (!StringUtils.isBlank(parm)) setTenantBaseUrl(parm);
  
    parm = System.getenv("tapis.meta.service.skSvcURL");
    if (!StringUtils.isBlank(parm)) setSkSvcURL(parm);
  
    parm = System.getenv("tapis.meta.service.tokenBaseUrl");
    if (!StringUtils.isBlank(parm)) setTokenBaseUrl(parm);
  
  }
  
  /** Initialize the singleton instance of this class.
   *
   * @return the non-null singleton instance of this class
   */
  private static synchronized RuntimeParameters initInstance()
  {
    if (_instance == null) _instance = new RuntimeParameters();
    return _instance;
  }
  
  public static RuntimeParameters getInstance() {
    return _instance;
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
    // buf.append(getNetworkAddresses());
    
    buf.append("\n------- Tenants -----------------------------------");
    buf.append("\ntapis.tenant.svc.baseurl: ");
    
    buf.append("\n------- DB Configuration --------------------------");

/*
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
*/
    
/*
    buf.append("\n------- Support Configuration ---------------------");
    buf.append("\ntapis.support.name: ");
    // buf.append(this.getSupportName());
    buf.append("\ntapis.support.email: ");
    // buf.append(this.getSupportEmail());
*/
    
    buf.append("\n------- EnvOnly Configuration ---------------------");
    buf.append("\ntapis.envonly.log.security.info: ");
    buf.append(RuntimeParameters.getLogSecurityInfo());
    buf.append("\ntapis.envonly.allow.test.header.parms: ");
    // buf.append(this.isAllowTestHeaderParms());
    buf.append("\ntapis.envonly.jwt.optional: ");
    buf.append(TapisEnv.getBoolean(TapisEnv.EnvVar.TAPIS_ENVONLY_JWT_OPTIONAL));
    buf.append("\ntapis.envonly.skip.jwt.verify: ");
    buf.append(TapisEnv.getBoolean(TapisEnv.EnvVar.TAPIS_ENVONLY_SKIP_JWT_VERIFY));
    
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
  
  public String getSiteId() { return siteId; }
  
  public void setSiteId(String siteId) { this.siteId = siteId; }
  
  public String getTenantBaseUrl() { return this.tenantBaseUrl; }
  
  public void setTenantBaseUrl(String tenantBaseUrl) {
    this.tenantBaseUrl = tenantBaseUrl;
  }
  
  public String getSkSvcURL() { return skSvcURL; }
  
  public void setSkSvcURL(String skSvcURL) {
    this.skSvcURL = skSvcURL;
  }
  
  public String getTokenBaseUrl() { return tokenBaseUrl; }
  
  public void setTokenBaseUrl(String tokenBaseUrl) { this.tokenBaseUrl = tokenBaseUrl; }
  
  public String getMetaToken() {
    return metaToken;
  }
  
  public void setMetaToken(String metaToken) {
    this.metaToken = metaToken;
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
  
  public String getCoreServer() {
    return coreServer;
  }
  
  public void setCoreServer(String coreServer) {
    this.coreServer = coreServer;
  }
  
  public void setServiceJWT(){
    _log.debug("calling setServiceJWT ...");
    ServiceJWTParms serviceJWTParms = new ServiceJWTParms();
    serviceJWTParms.setAccessTTL(43200); // 12 hrs
    serviceJWTParms.setRefreshTTL(43200);
    serviceJWTParms.setServiceName(RuntimeParameters.SERVICE_USER_NAME);
    serviceJWTParms.setTenant(RuntimeParameters.SERVICE_TENANT_NAME);
    serviceJWTParms.setTokensBaseUrl(this.getTenantBaseUrl());
    // TODO see what this changed to
    serviceJWTParms.setTargetSites(Arrays.asList(site));
    serviceJWT = null;
    try {
      serviceJWT = new ServiceJWT(serviceJWTParms, TapisEnv.get(TapisEnv.EnvVar.TAPIS_SERVICE_PASSWORD));
    } catch (TapisException | TapisClientException e) {
      // TODO
      e.printStackTrace();
    }
  }

  public String getSeviceToken(){
    if((serviceJWT == null) || serviceJWT.hasExpiredAccessJWT(siteId)){
      setServiceJWT();
    }
    return serviceJWT.getAccessJWT(siteId);
  }
}
