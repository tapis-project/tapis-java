package edu.utexas.tacc.tapis.systems;

import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.AccessMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.TransferMethod;
import java.util.List;

/*
 * Protocol contains info for testing convenience
 */
public final class Protocol
{
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
    if (transferMethods1 != null) transferMethods = transferMethods1; else transferMethods = TSystem.DEFAULT_TRANSFER_METHODS;
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
}
