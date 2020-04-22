package edu.utexas.tacc.tapis.systems.model;

import edu.utexas.tacc.tapis.systems.model.TSystem.AccessMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.TransferMethod;

import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;

/*
 * Class representing an update to a Tapis System.
 * Fields set to null indicate attribute not updated.
 *
 * Make defensive copies as needed on get/set to keep this class as immutable as possible.
 */
public final class PatchSystem
{
  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************
  private String tenant;     // Name of the tenant for which the system is defined
  private String name;       // Name of the system
  private final String description; // Full description of the system
  private final String host;       // Host name or IP address
  private final Boolean enabled; // Indicates if systems is currently enabled
  private final String effectiveUserId; // User to use when accessing system, may be static or dynamic
  private final AccessMethod defaultAccessMethod; // How access authorization is handled by default
  private final List<TransferMethod> transferMethods; // Supported transfer methods, allowed values determined by system type
  private final Integer port;          // Port number used to access the system
  private final Boolean useProxy;  // Indicates if a system should be accessed through a proxy
  private final String proxyHost;  // Name or IP address of proxy host
  private final Integer proxyPort;     // Port number for proxy host
  private final List<Capability> jobCapabilities; // List of job related capabilities supported by the system
  private final String[] tags;       // List of arbitrary tags as strings
  private Object notes;      // Simple metadata as json

  // ************************************************************************
  // *********************** Constructors ***********************************
  // ************************************************************************

  /**
   * Constructor setting all final attributes.
   */
  public PatchSystem(String description1, String host1, Boolean enabled1, String effectiveUserId1,
                     AccessMethod defaultAccessMethod1, List<TransferMethod> transferMethods1,
                     Integer port1, Boolean useProxy1, String proxyHost1, Integer proxyPort1, List<Capability> jobCapabilities1,
                     String[] tags1, Object notes1)
  {
    description = description1;
    host = host1;
    enabled = enabled1;
    effectiveUserId = effectiveUserId1;
    defaultAccessMethod = defaultAccessMethod1;
    transferMethods = (transferMethods1 == null) ? null : new ArrayList<>(transferMethods1);
    port = port1;
    useProxy = useProxy1;
    proxyHost = proxyHost1;
    proxyPort = proxyPort1;
    jobCapabilities = (jobCapabilities1 == null) ? null : new ArrayList<>(jobCapabilities1);
    tags = (tags1 == null) ? null : tags1.clone();
    notes = notes1;
  }

  // ************************************************************************
  // *********************** Accessors **************************************
  // ************************************************************************
  public String getTenant() { return tenant; }
  public void setTenant(String s) { tenant = s; }

  public String getName() { return name; }
  public void setName(String s) { name = s; }

  public String getDescription() { return description; }

  public String getHost() { return host; }

  public Boolean isEnabled() { return enabled; }

  public String getEffectiveUserId() { return effectiveUserId; }

  public AccessMethod getDefaultAccessMethod() { return defaultAccessMethod; }

  public List<TransferMethod> getTransferMethods() {
    return (transferMethods == null) ? null : new ArrayList<>(transferMethods);
  }

  public Integer getPort() { return port; }

  public Boolean isUseProxy() { return useProxy; }

  public String getProxyHost() { return proxyHost; }

  public Integer getProxyPort() { return proxyPort; }

  public List<Capability> getJobCapabilities() {
    return (jobCapabilities == null) ? null : new ArrayList<>(jobCapabilities);
  }

  public String[] getTags() {
    return (tags == null) ? null : tags.clone();
  }

  public Object getNotes() {
    return notes;
  }
  public void setNotes(Object n) { notes = n; }
}
