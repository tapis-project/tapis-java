package edu.utexas.tacc.tapis.systems.api.requests;

import edu.utexas.tacc.tapis.systems.model.Capability;
import edu.utexas.tacc.tapis.systems.model.Credential;
import edu.utexas.tacc.tapis.systems.model.TSystem.AccessMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemType;
import edu.utexas.tacc.tapis.systems.model.TSystem.TransferMethod;

import java.util.List;

import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_EFFECTIVEUSERID;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_ENABLED;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_NOTES;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_OWNER;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_PORT;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_PROXYHOST;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_PROXYPORT;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_TAGS;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_TRANSFER_METHODS;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_USEPROXY;

/*
 * Class representing all system attributes that can be set in an incoming create request json body
 */
public final class ReqCreateSystem
{
  public String name;       // Name of the system
  public String description; // Full description of the system
  public SystemType systemType; // Type of system, e.g. LINUX, OBJECT_STORE
  public String owner = DEFAULT_OWNER;      // User who owns the system and has full privileges
  public String host;       // Host name or IP address
  public boolean enabled = DEFAULT_ENABLED; // Indicates if systems is currently enabled
  public String effectiveUserId = DEFAULT_EFFECTIVEUSERID; // User to use when accessing system, may be static or dynamic
  public AccessMethod defaultAccessMethod; // How access authorization is handled by default
  public Credential accessCredential; // Credential to be stored in or retrieved from the Security Kernel
  public String bucketName; // Name of bucket for system of type OBJECT_STORE
  public String rootDir;    // Effective root directory for system of type LINUX, can also be used for system of type OBJECT_STORE
  public List<TransferMethod> transferMethods = DEFAULT_TRANSFER_METHODS; // Supported transfer methods, allowed values determined by system type
  public int port = DEFAULT_PORT;          // Port number used to access the system
  public boolean useProxy = DEFAULT_USEPROXY;  // Indicates if a system should be accessed through a proxy
  public String proxyHost = DEFAULT_PROXYHOST;  // Name or IP address of proxy host
  public int proxyPort = DEFAULT_PROXYPORT;     // Port number for proxy host
  public boolean jobCanExec; // Indicates if system will be used to execute jobs
  public String jobLocalWorkingDir; // Parent directory from which jobs are run, inputs and application assets are staged
  public String jobLocalArchiveDir; // Parent directory used for archiving job output files
  public String jobRemoteArchiveSystem; // Remote system on which job output files will be archived
  public String jobRemoteArchiveDir; // Parent directory used for archiving job output files on remote system
  public List<Capability> jobCapabilities; // List of job related capabilities supported by the system
  public String[] tags = DEFAULT_TAGS;       // List of arbitrary tags as strings
  public Object notes = DEFAULT_NOTES;      // Simple metadata as json
  public String refImportId; // Optional reference ID for systems created via import
}
