package edu.utexas.tacc.tapis.systems.api.requestBody;

public final class CreateSystem
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
  public String accessCredential;
  public Protocol Protocol;

  class Protocol
  {
    public String accessMechanism;
    public int port;
    public boolean useProxy;
    public String proxyHost;
    public int proxyPort;
  }
}
