package edu.utexas.tacc.tapis.systems.api.requests;

import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.systems.model.Capability;
import edu.utexas.tacc.tapis.systems.model.Credential;
import edu.utexas.tacc.tapis.systems.model.TSystem.AccessMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.TransferMethod;

import java.util.List;

public final class ReqCreateSystem
{
  public String name;
  public String description;
  public String systemType;
  public String owner;
  public String host;
  public boolean enabled;
  public String effectiveUserId;
  public AccessMethod defaultAccessMethod;
  public Credential accessCredential;
  public String bucketName;
  public String rootDir;
  public List<TransferMethod> transferMethods;
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
  public JsonObject notes;
}
