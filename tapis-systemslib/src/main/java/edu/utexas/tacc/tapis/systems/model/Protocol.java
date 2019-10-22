package edu.utexas.tacc.tapis.systems.model;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Arrays;

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
  public static final AccessMechanism DEFAULT_ACCESS_MECHANISM = AccessMechanism.NONE;
  public static final TransferMechanism[] DEFAULT_TRANSFER_MECHANISMS = {};
  public static final int DEFAULT_PORT = -1;
  public static final boolean DEFAULT_USEPROXY = false;
  public static final String DEFAULT_PROXYHOST = "";
  public static final int DEFAULT_PROXYPORT = -1;

  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  // Logging
  private static final Logger _log = LoggerFactory.getLogger(Protocol.class);

  private final int id;
  private final AccessMechanism accessMechanism; // How access authorization is handled.
  private final TransferMechanism[] transferMechanisms; // List of supported transfer mechanisms
  private final int port; // Port number used to access a system.
  private final boolean useProxy; // Indicates if a system should be accessed through a proxy.
  private final String proxyHost; //
  private final int proxyPort; //
  private Instant created; // UTC time for when record was created

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */
  public Protocol(int id1, AccessMechanism accessMechanism1, TransferMechanism[] transferMechanisms1,
                  int port1, boolean useProxy1, String proxyHost1, int proxyPort1, Instant created1)
  {
    id = id1;
    accessMechanism = accessMechanism1;
    if (transferMechanisms1 != null) transferMechanisms = transferMechanisms1;
                                     else transferMechanisms = DEFAULT_TRANSFER_MECHANISMS;
    port = port1;
    useProxy = useProxy1;
    proxyHost = proxyHost1;
    proxyPort = proxyPort1;
    created = created1;
  }

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
  public int getId() { return id; }
  public String getAccessMechanism() { return accessMechanism.toString(); }
  public int getPort() { return port; }
  public boolean isUseProxy() { return useProxy; }
  public String getProxyHost() { return proxyHost; }
  public int getProxyPort() { return proxyPort; }

  public TransferMechanism[] getTransferMechanisms()
  {
//    String[] mechanisms = Arrays.stream(transferMechanisms).toArray(String[]::new);;
    return transferMechanisms;
  }

  @Schema(type = "string")
  public Instant getCreated() { return created; }

  @Override
  public String toString() {return TapisUtils.toString(this);}
}
