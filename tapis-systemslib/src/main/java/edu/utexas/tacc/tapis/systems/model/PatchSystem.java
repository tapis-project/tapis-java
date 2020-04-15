package edu.utexas.tacc.tapis.systems.model;

import edu.utexas.tacc.tapis.systems.model.TSystem.AccessMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.TransferMethod;

import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/*
 * Class representing an update to a Tapis System.
 *
 */
public final class PatchSystem
{
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************


  // ************************************************************************
  // *********************** Enums ******************************************
  // ************************************************************************

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************
  // Logging
  private static final Logger _log = LoggerFactory.getLogger(PatchSystem.class);

  private String tenant;     // Name of the tenant for which the system is defined
  private String name;       // Name of the system
  private String description; // Full description of the system
  private String host;       // Host name or IP address
  private boolean enabled; // Indicates if systems is currently enabled
  private String effectiveUserId; // User to use when accessing system, may be static or dynamic
  private AccessMethod defaultAccessMethod; // How access authorization is handled by default
  private List<TransferMethod> transferMethods; // Supported transfer methods, allowed values determined by system type
  private int port;          // Port number used to access the system
  private boolean useProxy;  // Indicates if a system should be accessed through a proxy
  private String proxyHost;  // Name or IP address of proxy host
  private int proxyPort;     // Port number for proxy host
  private List<Capability> jobCapabilities; // List of job related capabilities supported by the system
  private String[] tags;       // List of arbitrary tags as strings
  private JsonObject notes;      // Simple metadata as json


  // ************************************************************************
  // *********************** Constructors ***********************************
  // ************************************************************************

  /**
   * Constructor using all attributes. Useful for testing.
   */
  public PatchSystem(String description1, String host1, boolean enabled1, String effectiveUserId1,
                     AccessMethod defaultAccessMethod1, List<TransferMethod> transferMethods1,
                     int port1, boolean useProxy1, String proxyHost1, int proxyPort1, List<Capability> jobCapabilities1,
                     String[] tags1, JsonObject notes1)
  {
//    description = description1;
//    owner = owner1;
//    host = host1;
//    enabled = enabled1;
//    effectiveUserId = effectiveUserId1;
//    defaultAccessMethod = defaultAccessMethod1;
//    accessCredential = accessCredential1;
//    if (transferMethods1 != null) transferMethods = transferMethods1;
//    else transferMethods = DEFAULT_TRANSFER_METHODS;
//    port = port1;
//    useProxy = useProxy1;
//    proxyHost = proxyHost1;
//    proxyPort = proxyPort1;
//    if (jobCapabilities1 != null) jobCapabilities = jobCapabilities1;
//    else jobCapabilities = new ArrayList<>();
//    tags = tags1;
//    notes = notes1;
  }

  // ************************************************************************
  // *********************** Public methods *********************************
  // ************************************************************************
  public static PatchSystem checkAndSetDefaults(PatchSystem patchSystem)
  {
//    if (StringUtils.isBlank(system.getOwner())) system.setOwner(DEFAULT_OWNER);
//    if (StringUtils.isBlank(system.getEffectiveUserId())) system.setEffectiveUserId(DEFAULT_EFFECTIVEUSERID);
//    if (system.getTags() == null) system.setTags(DEFAULT_TAGS);
//    if (system.getNotes() == null) system.setNotes(DEFAULT_NOTES);
//    if (system.getTransferMethods() == null) system.setTransferMethods(DEFAULT_TRANSFER_METHODS);
    return patchSystem;
  }

  // ************************************************************************
  // *********************** Accessors **************************************
  // ************************************************************************
  public String getTenant() { return tenant; }
  public PatchSystem setTenant(String s) { tenant = s; return this; }

  public String getName() { return name; }
  public PatchSystem setName(String s) { name = s; return this; }

  public String getDescription() { return description; }
  public PatchSystem setDescription(String descr) { description = descr; return this; }

  public String getHost() { return host; }
  public PatchSystem setHost(String s) { host = s; return this; }

  public boolean isEnabled() { return enabled; }
  public PatchSystem setEnabled(boolean b) { enabled = b;  return this; }

  public String getEffectiveUserId() { return effectiveUserId; }
  public PatchSystem setEffectiveUserId(String userId) { effectiveUserId = userId; return this; }

  public AccessMethod getDefaultAccessMethod() { return defaultAccessMethod; }

  public List<TransferMethod> getTransferMethods() { return transferMethods; }
  public PatchSystem setTransferMethods(List<TransferMethod> t) { transferMethods = t; return this; }

  public int getPort() { return port; }
  public PatchSystem setPort(int i) { port = i; return this; }

  public boolean isUseProxy() { return useProxy; }
  public PatchSystem setUseProxy(boolean b) { useProxy = b; return this; }

  public String getProxyHost() { return proxyHost; }
  public PatchSystem setProxyHost(String s) { proxyHost = s; return this; }

  public int getProxyPort() { return proxyPort; }
  public PatchSystem setProxyPort(int i) { proxyPort = i; return this; }

  public List<Capability> getJobCapabilities() { return jobCapabilities; }
  public PatchSystem setJobCapabilities(List<Capability> c) { jobCapabilities = c; return this; }

  public String[] getTags() { return tags; }
  public PatchSystem setTags(String[] sa) { tags = sa; return this; }

  public JsonObject getNotes() { return notes; }
  public PatchSystem setNotes(JsonObject jo) { notes = jo; return this; }
}
