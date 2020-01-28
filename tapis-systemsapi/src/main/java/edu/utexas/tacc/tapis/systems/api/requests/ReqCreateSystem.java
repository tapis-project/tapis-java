package edu.utexas.tacc.tapis.systems.api.requests;

import edu.utexas.tacc.tapis.systems.model.Capability;
import edu.utexas.tacc.tapis.systems.model.Credential;

import java.util.List;

public final class ReqCreateSystem
{
  public String name;
  public String description;
  public String systemType;
  public String owner;
  public String host;
  public boolean available;
  public String effectiveUserId;
  public String accessMethod;
  public Credential accessCredential;
  public String bucketName;
  public String rootDir;
  public List<String> transferMethods;
  public int port;
  public boolean useProxy;
  public String proxyHost;
  public int proxyPort;
  public boolean jobCanExec;
  public String jobLocalWorkingDir;
  public String jobLocalArchiveDir;
  public String jobRemoteArchiveSystem;
  public String jobRemoteArchiveDir;
  public List<Capability> jobCapabilities;
  public String tags;
  public String notes;
}
