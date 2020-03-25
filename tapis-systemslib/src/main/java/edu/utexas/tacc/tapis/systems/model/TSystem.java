package edu.utexas.tacc.tapis.systems.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Tapis System representing a server or collection of servers exposed through a
 * single host name or ip address. Each system is associated with a specific tenant.
 * Name of the system must be URI safe, see RFC 3986.
 *   Allowed characters: Alphanumeric  [0-9a-zA-Z] and special characters [-._~].
 * Each system has an owner, effective access user, protocol attributes
 *   and flag indicating if it is currently enabled.
 *
 * Tenant + name must be unique
 */
public final class TSystem
{
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************

  // Allowed substitution variables
  public static final String APIUSERID_VAR = "${apiUserId}";
  public static final String OWNER_VAR = "${owner}";
  public static final String TENANT_VAR = "${tenant}";
  public static final String EFFUSERID_VAR = "${effectiveUserId}";
  public static final String PERMS_WILDCARD = "*";

  // Default values
  public static final String DEFAULT_OWNER = APIUSERID_VAR;
  public static final boolean DEFAULT_ENABLED = true;
  public static final String DEFAULT_EFFECTIVEUSERID = APIUSERID_VAR;
  public static final String DEFAULT_NOTES_STR = "{}";
  public static final String DEFAULT_TAGS_STR = "{}";
  public static final JsonObject DEFAULT_NOTES = JsonParser.parseString(DEFAULT_NOTES_STR).getAsJsonObject();
  public static final String[] DEFAULT_TAGS = new String[0];
  public static final List<TransferMethod> DEFAULT_TRANSFER_METHODS = Collections.emptyList();
  public static final String EMPTY_TRANSFER_METHODS_STR = "{}";
  public static final String DEFAULT_TRANSFER_METHODS_STR = EMPTY_TRANSFER_METHODS_STR;
  public static final int DEFAULT_PORT = -1;
  public static final boolean DEFAULT_USEPROXY = false;
  public static final String DEFAULT_PROXYHOST = "";
  public static final int DEFAULT_PROXYPORT = -1;

  // ************************************************************************
  // *********************** Enums ******************************************
  // ************************************************************************
  public enum SystemType {LINUX, OBJECT_STORE}
  public enum Permission {ALL, READ, MODIFY}
  public enum AccessMethod {PASSWORD, PKI_KEYS, ACCESS_KEY, CERT}
  public enum TransferMethod {SFTP, S3}

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************
  // Logging
  private static final Logger _log = LoggerFactory.getLogger(TSystem.class);

  private long id;           // Unique database sequence number
  private Instant created; // UTC time for when record was created
  private Instant updated; // UTC time for when record was last updated

  private String tenant;     // Name of the tenant for which the system is defined
  private String name;       // Name of the system
  private String description; // Full description of the system
  private SystemType systemType; // Type of system, e.g. LINUX, OBJECT_STORE
  private String owner;      // User who owns the system and has full privileges
  private String host;       // Host name or IP address
  private boolean enabled; // Indicates if systems is currently enabled
  private String effectiveUserId; // User to use when accessing system, may be static or dynamic
  private AccessMethod defaultAccessMethod; // How access authorization is handled by default
  private Credential accessCredential; // Credential to be stored in or retrieved from the Security Kernel
  private String bucketName; // Name of bucket for system of type OBJECT_STORE
  private String rootDir;    // Effective root directory for system of type LINUX, can also be used for system of type OBJECT_STORE
  private List<TransferMethod> transferMethods; // Supported transfer methods, allowed values determined by system type
  private int port;          // Port number used to access the system
  private boolean useProxy;  // Indicates if a system should be accessed through a proxy
  private String proxyHost;  // Name or IP address of proxy host
  private int proxyPort;     // Port number for proxy host
  private boolean jobCanExec; // Indicates if system will be used to execute jobs
  private String jobLocalWorkingDir; // Parent directory from which jobs are run, inputs and application assets are staged
  private String jobLocalArchiveDir; // Parent directory used for archiving job output files
  private String jobRemoteArchiveSystem; // Remote system on which job output files will be archived
  private String jobRemoteArchiveDir; // Parent directory used for archiving job output files on remote system
  private List<Capability> jobCapabilities; // List of job related capabilities supported by the system
  private String[] tags;       // List of arbitrary tags as strings
  private JsonObject notes;      // Simple metadata as json


  // ************************************************************************
  // *********************** Constructors ***********************************
  // ************************************************************************

  /**
   * Constructor using only required attributes.
   */
  public TSystem(String name1, SystemType systemType1, String host1, AccessMethod defaultAccessMethod1, boolean jobCanExec1)
  {
    name = name1;
    systemType = systemType1;
    host = host1;
    defaultAccessMethod = defaultAccessMethod1;
    jobCanExec = jobCanExec1;
    owner = DEFAULT_OWNER;
    enabled = DEFAULT_ENABLED;
    effectiveUserId = DEFAULT_EFFECTIVEUSERID;
    port = DEFAULT_PORT;
    useProxy = DEFAULT_USEPROXY;
    proxyHost = DEFAULT_PROXYHOST;
    proxyPort = DEFAULT_PORT;
    notes = TapisGsonUtils.getGson().fromJson(DEFAULT_NOTES_STR, JsonObject.class);
  }

  /**
   * Constructor using all attributes. Useful for testing.
   */
  public TSystem(long id1, String tenant1, String name1, String description1, SystemType systemType1,
                 String owner1, String host1, boolean enabled1, String effectiveUserId1, AccessMethod defaultAccessMethod1,
                 Credential accessCredential1, String bucketName1, String rootDir1,
                 List<TransferMethod> transferMethods1, int port1, boolean useProxy1, String proxyHost1, int proxyPort1,
                 boolean jobCanExec1, String jobLocalWorkingDir1, String jobLocalArchiveDir1,
                 String jobRemoteArchiveSystem1, String jobRemoteArchiveDir1, List<Capability> jobCapabilities1,
                 String[] tags1, JsonObject notes1, Instant created1, Instant updated1)
  {
    id = id1;
    created = created1;
    updated = updated1;
    tenant = tenant1;
    name = name1;
    description = description1;
    systemType = systemType1;
    owner = owner1;
    host = host1;
    enabled = enabled1;
    effectiveUserId = effectiveUserId1;
    defaultAccessMethod = defaultAccessMethod1;
    accessCredential = accessCredential1;
    bucketName = bucketName1;
    rootDir = rootDir1;
    if (transferMethods1 != null) transferMethods = transferMethods1;
    else transferMethods = DEFAULT_TRANSFER_METHODS;
    port = port1;
    useProxy = useProxy1;
    proxyHost = proxyHost1;
    proxyPort = proxyPort1;
    jobCanExec = jobCanExec1;
    jobLocalWorkingDir = jobLocalWorkingDir1;
    jobLocalArchiveDir = jobLocalArchiveDir1;
    jobRemoteArchiveSystem = jobRemoteArchiveSystem1;
    jobRemoteArchiveDir = jobRemoteArchiveDir1;
    if (jobCapabilities1 != null) jobCapabilities = jobCapabilities1;
    else jobCapabilities = new ArrayList<>();
    tags = tags1;
    notes = notes1;
  }

  // ************************************************************************
  // *********************** Public methods *********************************
  // ************************************************************************
  public static TSystem checkAndSetDefaults(TSystem system)
  {
    if (StringUtils.isBlank(system.getOwner())) system.setOwner(DEFAULT_OWNER);
    if (StringUtils.isBlank(system.getEffectiveUserId())) system.setEffectiveUserId(DEFAULT_EFFECTIVEUSERID);
    if (system.getTags() == null) system.setTags(DEFAULT_TAGS);
    if (system.getNotes() == null) system.setNotes(DEFAULT_NOTES);
    if (system.getTransferMethods() == null) system.setTransferMethods(DEFAULT_TRANSFER_METHODS);
    return system;
  }

  // ************************************************************************
  // *********************** Accessors **************************************
  // ************************************************************************
  public long getId() { return id; }

  @Schema(type = "string")
  public Instant getCreated() { return created; }

  @Schema(type = "string")
  public Instant getUpdated() { return updated; }

  public String getTenant() { return tenant; }
  public TSystem setTenant(String s) { tenant = s; return this; }

  public String getName() { return name; }
  public TSystem setName(String s) { name = s; return this; }

  public String getDescription() { return description; }
  public TSystem setDescription(String descr) { description = descr; return this; }

  public SystemType getSystemType() { return systemType; }
  public TSystem setSystemType(SystemType st) { systemType = st; return this; }

  public String getOwner() { return owner; }
  public TSystem setOwner(String s) { owner = s;  return this;}

  public String getHost() { return host; }
  public TSystem setHost(String s) { host = s; return this; }

  public boolean isEnabled() { return enabled; }
  public TSystem setEnabled(boolean b) { enabled = b;  return this; }

  public String getEffectiveUserId() { return effectiveUserId; }
  public TSystem setEffectiveUserId(String userId) { effectiveUserId = userId; return this; }

  public AccessMethod getDefaultAccessMethod() { return defaultAccessMethod; }

  public Credential getAccessCredential() { return accessCredential; }
  public TSystem setAccessCredential(Credential cred) {accessCredential = cred; return this; }

  public String getBucketName() { return bucketName; }
  public TSystem setBucketName(String s) { bucketName = s; return this; }

  public String getRootDir() { return rootDir; }
  public TSystem setRootDir(String s) { rootDir = s; return this; }

  public List<TransferMethod> getTransferMethods() { return transferMethods; }
  public TSystem setTransferMethods(List<TransferMethod> t) { transferMethods = t; return this; }

  public int getPort() { return port; }
  public TSystem setPort(int i) { port = i; return this; }

  public boolean isUseProxy() { return useProxy; }
  public TSystem setUseProxy(boolean b) { useProxy = b; return this; }

  public String getProxyHost() { return proxyHost; }
  public TSystem setProxyHost(String s) { proxyHost = s; return this; }

  public int getProxyPort() { return proxyPort; }
  public TSystem setProxyPort(int i) { proxyPort = i; return this; }

  public boolean getJobCanExec() { return jobCanExec; }
  public TSystem setJobCanExec(boolean b) { jobCanExec = b;  return this; }

  public String getJobLocalWorkingDir() { return jobLocalWorkingDir; }
  public TSystem setJobLocalWorkingDir(String s) { jobLocalWorkingDir = s; return this; }

  public String getJobLocalArchiveDir() { return jobLocalArchiveDir; }
  public TSystem setJobLocalArchiveDir(String s) { jobLocalArchiveDir = s; return this; }

  public String getJobRemoteArchiveSystem() { return jobRemoteArchiveSystem; }

  public String getJobRemoteArchiveDir() { return jobRemoteArchiveDir; }
  public TSystem setJobRemoteArchiveDir(String s) { jobRemoteArchiveDir = s; return this; }

  public List<Capability> getJobCapabilities() { return jobCapabilities; }
  public TSystem setJobCapabilities(List<Capability> c) { jobCapabilities = c; return this; }

  public String[] getTags() { return tags; }
  public TSystem setTags(String[] sa) { tags = sa; return this; }

  public JsonObject getNotes() { return notes; }
  public TSystem setNotes(JsonObject jo) { notes = jo; return this; }
}
