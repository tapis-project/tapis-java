package edu.utexas.tacc.tapis.systems.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonObject;
import io.swagger.v3.oas.annotations.media.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.systems.model.Protocol.AccessMethod;
import edu.utexas.tacc.tapis.systems.model.Protocol.TransferMethod;

import static edu.utexas.tacc.tapis.systems.model.Protocol.DEFAULT_TRANSFER_METHODS;

/*
 * Tapis System representing a server or collection of servers exposed through a
 * single host name or ip address. Each system is associated with a specific tenant.
 * Name of the system must be URI safe, see RFC 3986.
 *   Allowed characters: Alphanumeric  [0-9a-zA-Z] and special characters [-._~].
 * Each system has an owner, effective access user, protocol attributes
 *   and flag indicating if it is currently available.
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
  public static final boolean DEFAULT_AVAILABLE = true;
  public static final String DEFAULT_EFFECTIVEUSERID = APIUSERID_VAR;
  public static final boolean DEFAULT_USEPROXY = false;
  public static final String DEFAULT_TAGS_STR = "{}";
  public static final String DEFAULT_NOTES_STR = "{}";

  // ************************************************************************
  // *********************** Enums ******************************************
  // ************************************************************************
  public enum SystemType {LINUX, OBJECT_STORE}
  public enum Permission {READ, MODIFY, DELETE}

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
  private boolean available; // Indicates if systems is currently available
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
  private JsonObject tags;       // Simple metadata as json containing key:val pairs for efficient searching
  private JsonObject notes;      // Simple metadata as json


  // ************************************************************************
  // *********************** Constructors ***********************************
  // ************************************************************************
  public TSystem(long id1, String tenant1, String name1, String description1, SystemType systemType1,
                 String owner1, String host1, boolean available1, String effectiveUserId1, AccessMethod defaultAccessMethod1,
                 Credential accessCredential1, String bucketName1, String rootDir1,
                 List<TransferMethod> transferMethods1, int port1, boolean useProxy1, String proxyHost1, int proxyPort1,
                 boolean jobCanExec1, String jobLocalWorkingDir1, String jobLocalArchiveDir1,
                 String jobRemoteArchiveSystem1, String jobRemoteArchiveDir1, List<Capability> jobCapabilities1,
                 JsonObject tags1, JsonObject notes1, Instant created1, Instant updated1)
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
    available = available1;
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
  // *********************** Accessors **************************************
  // ************************************************************************
  public long getId() { return id; }

  @Schema(type = "string")
  public Instant getCreated() { return created; }

  @Schema(type = "string")
  public Instant getUpdated() { return updated; }

  public String getTenant() { return tenant; }
  public void setTenant(String s) { tenant = s; }

  public String getName() { return name; }

  public String getDescription() { return description; }
  public void setDescription(String descr) { description = descr; }

  public SystemType getSystemType() { return systemType; }

  public String getOwner() { return owner; }
  public void setOwner(String s) { owner = s; }

  public String getHost() { return host; }

  public boolean isAvailable() { return available; }
  public void setAvailable(boolean avail) { available = avail; }

  public String getEffectiveUserId() { return effectiveUserId; }
  public void setEffectiveUserId(String userId) { effectiveUserId = userId; }

  public AccessMethod getDefaultAccessMethod() { return defaultAccessMethod; }

  public Credential getAccessCredential() { return accessCredential; }
  public void setAccessCredential(Credential cred) {accessCredential = cred;}

  public String getBucketName() { return bucketName; }
  public void setBucketName(String s) { bucketName = s; }

  public String getRootDir() { return rootDir; }
  public void setRootDir(String s) { rootDir = s;}

  public List<TransferMethod> getTransferMethods() { return transferMethods; }

  public int getPort() { return port; }

  public boolean isUseProxy() { return useProxy; }

  public String getProxyHost() { return proxyHost; }

  public int getProxyPort() { return proxyPort; }

  public boolean getJobCanExec() { return jobCanExec; }

  public String getJobLocalWorkingDir() { return jobLocalWorkingDir; }
  public void setJobLocalWorkingDir(String s) { jobLocalWorkingDir = s; }

  public String getJobLocalArchiveDir() { return jobLocalArchiveDir; }
  public void setJobLocalArchiveDir(String s) { jobLocalArchiveDir = s; }

  public String getJobRemoteArchiveSystem() { return jobRemoteArchiveSystem; }

  public String getJobRemoteArchiveDir() { return jobRemoteArchiveDir; }
  public void setJobRemoteArchiveDir(String s) { jobRemoteArchiveDir = s; }

  public List<Capability> getJobCapabilities() { return jobCapabilities; }
  public void setJobCapabilities(List<Capability> c) { jobCapabilities = c; }

  public JsonObject getTags() { return tags; }

  public JsonObject getNotes() { return notes; }
}
