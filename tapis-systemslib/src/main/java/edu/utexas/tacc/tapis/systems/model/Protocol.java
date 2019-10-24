package edu.utexas.tacc.tapis.systems.model;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/*
 * Protocol contains information required to access a system, except for user and secret.
 * It also contains a list of supported transfer mechanisms
 * This class is intended to represent an immutable object.
 * Please keep it immutable.
 */
public final class Protocol
{
  public enum AccessMechanism  {NONE, ANONYMOUS, SSH_PASSWORD, SSH_KEYS, SSH_CERT}
  public enum TransferMechanism {SFTP, S3, LOCAL}

  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  private static final String EMPTY_TRANSFER_MECHANISMS_STR = "{}";
  public static final AccessMechanism DEFAULT_ACCESS_MECHANISM = AccessMechanism.NONE;
  public static final List<TransferMechanism> DEFAULT_TRANSFER_MECHANISMS = Collections.emptyList();
  public static final String DEFAULT_TRANSFER_MECHANISMS_STR = EMPTY_TRANSFER_MECHANISMS_STR;
  public static final int DEFAULT_PORT = -1;
  public static final boolean DEFAULT_USEPROXY = false;
  public static final String DEFAULT_PROXYHOST = "";
  public static final int DEFAULT_PROXYPORT = -1;

  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  // Logging
  private static final Logger _log = LoggerFactory.getLogger(Protocol.class);

  private final AccessMechanism accessMechanism; // How access authorization is handled.
  private final List<TransferMechanism> transferMechanisms; // List of supported transfer mechanisms
  private final int port; // Port number used to access a system.
  private final boolean useProxy; // Indicates if a system should be accessed through a proxy.
  private final String proxyHost; //
  private final int proxyPort; //

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */
  public Protocol(AccessMechanism accessMechanism1, List<TransferMechanism> transferMechanisms1,
                  int port1, boolean useProxy1, String proxyHost1, int proxyPort1)
  {
    accessMechanism = accessMechanism1;
    if (transferMechanisms1 != null) transferMechanisms = transferMechanisms1;
                                     else transferMechanisms = DEFAULT_TRANSFER_MECHANISMS;
    port = port1;
    useProxy = useProxy1;
    proxyHost = proxyHost1;
    proxyPort = proxyPort1;
  }

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
  public AccessMechanism getAccessMechanism() { return accessMechanism; }
  public int getPort() { return port; }
  public boolean isUseProxy() { return useProxy; }
  public String getProxyHost() { return proxyHost; }
  public int getProxyPort() { return proxyPort; }
  public List<TransferMechanism> getTransferMechanisms() { return transferMechanisms; }

  /**
   * Return List of transfer mechanisms as a comma delimited list of strings surrounded by curly braces.
   * @return
   */
  public String getTransferMechanismsAsStr()
  {
    if (transferMechanisms == null || transferMechanisms.size() == 0) return EMPTY_TRANSFER_MECHANISMS_STR;
    StringBuilder sb = new StringBuilder("{");
    for (int i = 0; i < transferMechanisms.size()-1; i++)
    {
      sb.append(transferMechanisms.get(i).name()).append(",");
    }
    sb.append(transferMechanisms.get(transferMechanisms.size()-1).name());
    sb.append("}");
    return sb.toString();
  }

  @Override
  public String toString() {return TapisUtils.toString(this);}
}
