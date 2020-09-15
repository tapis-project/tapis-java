package edu.utexas.tacc.tapis.systems.api.requests;

import edu.utexas.tacc.tapis.systems.model.Capability;
import edu.utexas.tacc.tapis.systems.model.Credential;
import edu.utexas.tacc.tapis.systems.model.TSystem.AccessMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemType;
import edu.utexas.tacc.tapis.systems.model.TSystem.TransferMethod;

import java.util.List;

/*
 * Class representing all system attributes that can be set in an incoming create request json body
 */
public final class ReqCreateSystem
{
  public String name;       // Name of the system
  public String description; // Full description of the system
  public SystemType systemType; // Type of system, e.g. LINUX, OBJECT_STORE
  public String owner;      // User who owns the system and has full privileges
  public String host;       // Host name or IP address
  public boolean enabled; // Indicates if systems is currently enabled
  public String effectiveUserId; // User to use when accessing system, may be static or dynamic
  public AccessMethod defaultAccessMethod; // How access authorization is handled by default
  public Credential accessCredential; // Credential to be stored in or retrieved from the Security Kernel
  public String bucketName; // Name of bucket for system of type OBJECT_STORE
  public String rootDir;    // Effective root directory for system of type LINUX, can also be used for system of type OBJECT_STORE
  public List<TransferMethod> transferMethods; // Supported transfer methods, allowed values determined by system type
  public int port;          // Port number used to access the system
  public boolean useProxy;  // Indicates if a system should be accessed through a proxy
  public String proxyHost;  // Name or IP address of proxy host
  public int proxyPort;     // Port number for proxy host
  public boolean jobCanExec; // Indicates if system will be used to execute jobs
  public String jobLocalWorkingDir; // Parent directory from which jobs are run, inputs and application assets are staged
  public String jobLocalArchiveDir; // Parent directory used for archiving job output files
  public String jobRemoteArchiveSystem; // Remote system on which job output files will be archived
  public String jobRemoteArchiveDir; // Parent directory used for archiving job output files on remote system
  public List<Capability> jobCapabilities; // List of job related capabilities supported by the system
  public String[] tags;       // List of arbitrary tags as strings
  public Object notes;      // Simple metadata as json
  public String refImportId; // Optional reference ID for systems created via import
}
