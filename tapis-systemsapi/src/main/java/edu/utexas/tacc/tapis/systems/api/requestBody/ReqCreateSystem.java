package edu.utexas.tacc.tapis.systems.api.requestBody;

import edu.utexas.tacc.tapis.systems.model.CommandProtocol;

public final class ReqCreateSystem
{
  public String name;
  public String description;
  public String owner;
  public String host;
  public boolean available;
  public String bucketName;
  public String rootDir;
  public String jobInputDir;
  public String jobOutputDir;
  public String scratchDir;
  public String workDir;
  public String effectiveUserId;
  public String commandCredential;
  public String transferCredential;
  public CommandProtocol commandProtocol;
  public TransferProtocol transferProtocol;

  class CommandProtocol
  {
    public String mechanism;
    public int port;
    public boolean useProxy;
    public String proxyHost;
    public int proxyPort;
  }

  class TransferProtocol
  {
    public String mechanism;
    public int port;
    public boolean useProxy;
    public String proxyHost;
    public int proxyPort;
  }
}
