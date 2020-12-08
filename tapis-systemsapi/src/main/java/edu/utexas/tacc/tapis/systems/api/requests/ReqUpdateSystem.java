package edu.utexas.tacc.tapis.systems.api.requests;

import edu.utexas.tacc.tapis.systems.model.Capability;
import edu.utexas.tacc.tapis.systems.model.LogicalQueue;
import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.TransferMethod;

import java.util.List;

import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_JOBMAXJOBS;
import static edu.utexas.tacc.tapis.systems.model.TSystem.DEFAULT_JOBMAXJOBSPERUSER;

/*
 * Class representing all system attributes that can be set in an incoming patch request json body
 */
public final class ReqUpdateSystem
{
  public String description; // Full description of the system
  public String host;       // Host name or IP address
  public Boolean enabled; // Indicates if systems is currently enabled
  public String effectiveUserId; // User to use when accessing system, may be static or dynamic
  public AuthnMethod defaultAuthnMethod; // How access authorization is handled by default
  public List<TransferMethod> transferMethods; // Supported transfer methods, allowed values determined by system type
  public Integer port;          // Port number used to access the system
  public Boolean useProxy;  // Indicates if a system should be accessed through a proxy
  public String proxyHost;  // Name or IP address of proxy host
  public Integer proxyPort;     // Port number for proxy host
  public String dtnSystemId;
  public String dtnMountPoint;
  public String dtnSubDir;
  public String jobWorkingDir; // Parent directory from which jobs are run, inputs and application assets are staged
  public String[] jobEnvVariables;
  public int jobMaxJobs = DEFAULT_JOBMAXJOBS;
  public int jobMaxJobsPerUser = DEFAULT_JOBMAXJOBSPERUSER;
  public boolean jobIsBatch;
  public String batchScheduler;
  public List<LogicalQueue> batchLogicalQueues;
  public String batchDefaultLogicalQueue;
  public List<Capability> jobCapabilities; // List of job related capabilities supported by the system
  public String[] tags;       // List of arbitrary tags as strings
  public Object notes;      // Simple metadata as json
}
