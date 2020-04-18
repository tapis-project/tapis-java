package edu.utexas.tacc.tapis.systems.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.utexas.tacc.tapis.systems.utils.LibUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.commons.lang3.StringUtils;

/*
 * Tapis System representing a server or collection of servers exposed through a
 * single host name or ip address. Each system is associated with a specific tenant.
 * Name of the system must be URI safe, see RFC 3986.
 *   Allowed characters: Alphanumeric  [0-9a-zA-Z] and special characters [-._~].
 * Each system has an owner, effective access user, protocol attributes
 *   and flag indicating if it is currently enabled.
 *
 * Tenant + name must be unique
 *
 * Make defensive copies as needed on get/set to keep this class as immutable as possible.
 * Note Credential is immutable so no need for copy.
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

  private int id;           // Unique database sequence number
  private Instant created; // UTC time for when record was created
  private Instant updated; // UTC time for when record was last updated

  private String tenant;     // Name of the tenant for which the system is defined
  private String name;       // Name of the system
  private String description; // Full description of the system
  private final SystemType systemType; // Type of system, e.g. LINUX, OBJECT_STORE
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
  private final boolean jobCanExec; // Indicates if system will be used to execute jobs
  private String jobLocalWorkingDir; // Parent directory from which jobs are run, inputs and application assets are staged
  private String jobLocalArchiveDir; // Parent directory used for archiving job output files
  private String jobRemoteArchiveSystem; // Remote system on which job output files will be archived
  private String jobRemoteArchiveDir; // Parent directory used for archiving job output files on remote system
  private List<Capability> jobCapabilities; // List of job related capabilities supported by the system
  private String[] tags;       // List of arbitrary tags as strings
  private Notes notes;      // Simple metadata as json


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
  }

  /**
   * Constructor using all attributes. Useful for testing.
   * Make defensive copies as needed. Note Credential is immutable so no need for copy.
   */
  public TSystem(int id1, String tenant1, String name1, String description1, SystemType systemType1,
                 String owner1, String host1, boolean enabled1, String effectiveUserId1, AccessMethod defaultAccessMethod1,
                 Credential accessCredential1, String bucketName1, String rootDir1,
                 List<TransferMethod> transferMethods1, int port1, boolean useProxy1, String proxyHost1, int proxyPort1,
                 boolean jobCanExec1, String jobLocalWorkingDir1, String jobLocalArchiveDir1,
                 String jobRemoteArchiveSystem1, String jobRemoteArchiveDir1, List<Capability> jobCapabilities1,
                 String[] tags1, Notes notes1, Instant created1, Instant updated1)
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
    transferMethods = (transferMethods1 == null) ? null : new ArrayList<>(transferMethods1);
    port = port1;
    useProxy = useProxy1;
    proxyHost = proxyHost1;
    proxyPort = proxyPort1;
    jobCanExec = jobCanExec1;
    jobLocalWorkingDir = jobLocalWorkingDir1;
    jobLocalArchiveDir = jobLocalArchiveDir1;
    jobRemoteArchiveSystem = jobRemoteArchiveSystem1;
    jobRemoteArchiveDir = jobRemoteArchiveDir1;
    jobCapabilities = (jobCapabilities1 == null) ? null : new ArrayList<>(jobCapabilities1);
    tags = (tags1 == null) ? null : tags1.clone();
    notes = notes1;
  }

  /**
   * Copy constructor. Returns a deep copy of a TSystem object.
   * Make defensive copies as needed. Note Credential is immutable so no need for copy.
   */
  public TSystem(TSystem t)
  {
    if (t==null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT"));
    id = t.getId();
    created = t.getCreated();
    updated = t.getUpdated();
    tenant = t.getTenant();
    name = t.getName();
    description = t.getDescription();
    systemType = t.getSystemType();
    owner = t.getOwner();
    host = t.getHost();
    enabled = t.isEnabled();
    effectiveUserId = t.getEffectiveUserId();
    defaultAccessMethod = t.getDefaultAccessMethod();
    accessCredential = t.getAccessCredential();
    bucketName = t.getBucketName();
    rootDir = t.getRootDir();
    transferMethods =  (t.getTransferMethods() == null) ? null : new ArrayList<>(t.getTransferMethods());
    port = t.getPort();
    useProxy = t.isUseProxy();
    proxyHost = t.getProxyHost();
    proxyPort = t.getProxyPort();
    jobCanExec = t.getJobCanExec();
    jobLocalWorkingDir = t.getJobLocalWorkingDir();
    jobLocalArchiveDir = t.getJobLocalArchiveDir();
    jobRemoteArchiveSystem = t.getJobRemoteArchiveSystem();
    jobRemoteArchiveDir = t.getJobRemoteArchiveDir();
    jobCapabilities = (t.getJobCapabilities() == null) ? null :  new ArrayList<>(t.getJobCapabilities());
    tags = (t.getTags() == null) ? null : t.getTags().clone();
    notes = t.getNotes();
  }

  // ************************************************************************
  // *********************** Public methods *********************************
  // ************************************************************************
  public static TSystem checkAndSetDefaults(TSystem system)
  {
    if (system==null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT"));
    if (StringUtils.isBlank(system.getOwner())) system.setOwner(DEFAULT_OWNER);
    if (StringUtils.isBlank(system.getEffectiveUserId())) system.setEffectiveUserId(DEFAULT_EFFECTIVEUSERID);
    if (system.getTags() == null) system.setTags(DEFAULT_TAGS);
    if (system.getNotes() == null || StringUtils.isBlank(system.getNotes().getStringData()))
    {
      system.setNotes(new Notes(DEFAULT_NOTES_STR));
    }
    if (system.getTransferMethods() == null) system.setTransferMethods(DEFAULT_TRANSFER_METHODS);
    return system;
  }

  // ************************************************************************
  // *********************** Accessors **************************************
  // ************************************************************************
  public int getId() { return id; }

  @Schema(type = "string")
  public Instant getCreated() { return created; }

  @Schema(type = "string")
  public Instant getUpdated() { return updated; }

  public String getTenant() { return tenant; }
  public TSystem setTenant(String s) { tenant = s; return this; }

  public String getName() { return name; }
  public TSystem setName(String s) { name = s; return this; }

  public String getDescription() { return description; }
  public TSystem setDescription(String d) { description = d; return this; }

  public SystemType getSystemType() { return systemType; }

  public String getOwner() { return owner; }
  public TSystem setOwner(String s) { owner = s;  return this;}

  public String getHost() { return host; }
  public TSystem setHost(String s) { host = s; return this; }

  public boolean isEnabled() { return enabled; }
  public TSystem setEnabled(boolean b) { enabled = b;  return this; }

  public String getEffectiveUserId() { return effectiveUserId; }
  public TSystem setEffectiveUserId(String s) { effectiveUserId = s; return this; }

  public AccessMethod getDefaultAccessMethod() { return defaultAccessMethod; }
  public TSystem setDefaultAccessMethod(AccessMethod a) { defaultAccessMethod = a; return this; }

  public Credential getAccessCredential() { return accessCredential; }
  public TSystem setAccessCredential(Credential c) {accessCredential = c; return this; }

  public String getBucketName() { return bucketName; }
  public TSystem setBucketName(String s) { bucketName = s; return this; }

  public String getRootDir() { return rootDir; }
  public TSystem setRootDir(String s) { rootDir = s; return this; }

  public List<TransferMethod> getTransferMethods() {
    return (transferMethods == null) ? null : new ArrayList<>(transferMethods);
  }
  public TSystem setTransferMethods(List<TransferMethod> t) {
    transferMethods = (t == null) ? null : new ArrayList<>(t);
    return this;
  }

  public int getPort() { return port; }
  public TSystem setPort(int i) { port = i; return this; }

  public boolean isUseProxy() { return useProxy; }
  public TSystem setUseProxy(boolean b) { useProxy = b; return this; }

  public String getProxyHost() { return proxyHost; }
  public TSystem setProxyHost(String s) { proxyHost = s; return this; }

  public int getProxyPort() { return proxyPort; }
  public TSystem setProxyPort(int i) { proxyPort = i; return this; }

  public boolean getJobCanExec() { return jobCanExec; }

  public String getJobLocalWorkingDir() { return jobLocalWorkingDir; }
  public TSystem setJobLocalWorkingDir(String s) { jobLocalWorkingDir = s; return this; }

  public String getJobLocalArchiveDir() { return jobLocalArchiveDir; }
  public TSystem setJobLocalArchiveDir(String s) { jobLocalArchiveDir = s; return this; }

  public String getJobRemoteArchiveSystem() { return jobRemoteArchiveSystem; }

  public String getJobRemoteArchiveDir() { return jobRemoteArchiveDir; }
  public TSystem setJobRemoteArchiveDir(String s) { jobRemoteArchiveDir = s; return this; }

  public List<Capability> getJobCapabilities() {
    return (jobCapabilities == null) ? null : new ArrayList<>(jobCapabilities);
  }
  public TSystem setJobCapabilities(List<Capability> c) {
    jobCapabilities = (c == null) ? null : new ArrayList<>(c);
    return this;
  }

  public String[] getTags() {
    return (tags == null) ? null : tags.clone();
  }
  public TSystem setTags(String[] t) {
    tags = (t == null) ? null : t.clone();
    return this;
  }

  public Notes getNotes() { return notes; }
  public TSystem setNotes(Notes n) { notes = n; return this; }
}
