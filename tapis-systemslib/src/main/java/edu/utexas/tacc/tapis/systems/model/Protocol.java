package edu.utexas.tacc.tapis.systems.model;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/*
 * Protocol contains information required to access a system, except for user and secret.
 * It also contains a list of supported transfer methods.
 * This class is intended to represent an immutable object.
 * Please keep it immutable.
 */
public final class Protocol
{
  public enum AccessMethod {PASSWORD, PKI_KEYS, CERT, ACCESS_KEY}
  public enum TransferMethod {SFTP, S3}

  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  private static final String EMPTY_TRANSFER_METHODS_STR = "{}";
  public static final AccessMethod DEFAULT_ACCESS_METHOD = AccessMethod.PASSWORD;
  public static final List<TransferMethod> DEFAULT_TRANSFER_METHODS = Collections.emptyList();
  public static final String DEFAULT_TRANSFER_METHODS_STR = EMPTY_TRANSFER_METHODS_STR;
  public static final int DEFAULT_PORT = -1;
  public static final boolean DEFAULT_USEPROXY = false;
  public static final String DEFAULT_PROXYHOST = "";
  public static final int DEFAULT_PROXYPORT = -1;

  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  // Logging
  private static final Logger _log = LoggerFactory.getLogger(Protocol.class);

  private final AccessMethod accessMethod; // How access authorization is handled.
  private final List<TransferMethod> transferMethods; // List of supported transfer methods
  private final int port; // Port number used to access a system.
  private final boolean useProxy; // Indicates if a system should be accessed through a proxy.
  private final String proxyHost; //
  private final int proxyPort; //

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */
  public Protocol(AccessMethod accessMethod1, List<TransferMethod> transferMethods1,
                  int port1, boolean useProxy1, String proxyHost1, int proxyPort1)
  {
    accessMethod = accessMethod1;
    if (transferMethods1 != null) transferMethods = transferMethods1;
                                     else transferMethods = DEFAULT_TRANSFER_METHODS;
    port = port1;
    useProxy = useProxy1;
    proxyHost = proxyHost1;
    proxyPort = proxyPort1;
  }

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
  public AccessMethod getAccessMethod() { return accessMethod; }
  public int getPort() { return port; }
  public boolean isUseProxy() { return useProxy; }
  public String getProxyHost() { return proxyHost; }
  public int getProxyPort() { return proxyPort; }
  public List<TransferMethod> getTransferMethods() { return transferMethods; }

  /**
   * Return List of transfer methods as a comma delimited list of strings surrounded by curly braces.
   * @return
   */
  public static String getTransferMethodsAsString(List<TransferMethod> txfrMethods)
  {
    if (txfrMethods == null || txfrMethods.size() == 0) return EMPTY_TRANSFER_METHODS_STR;
    StringBuilder sb = new StringBuilder("{");
    for (int i = 0; i < txfrMethods.size()-1; i++)
    {
      sb.append(txfrMethods.get(i).name()).append(",");
    }
    sb.append(txfrMethods.get(txfrMethods.size()-1).name());
    sb.append("}");
    return sb.toString();
  }

  @Override
  public String toString() {return TapisUtils.toString(this);}
}
