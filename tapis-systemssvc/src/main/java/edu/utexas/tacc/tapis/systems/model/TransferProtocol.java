package edu.utexas.tacc.tapis.systems.model;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/*
 * Transfer Protocol contains information required to do I/O for a system, except for user and secret.
 * This class is intended to represent an immutable object.
 * Please keep it that way.
 */
public final class TransferProtocol
{
  public enum Mechanism {NONE, SFTP, S3, LOCAL}

  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  public static final Mechanism DEFAULT_MECHANISM= Mechanism.NONE;
  public static final int DEFAULT_PORT = -1;
  public static final boolean DEFAULT_USEPROXY = false;
  public static final String DEFAULT_PROXYHOST = "";
  public static final int DEFAULT_PROXYPORT = -1;

  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  // Logging
  private static final Logger _log = LoggerFactory.getLogger(TransferProtocol.class);

  private final int id;
  private final Mechanism mechanism; // How access authorization is handled.
  private final int port; // Port number used to access a system.
  private final boolean useProxy; // Indicates if a system should be accessed through a proxy.
  private final String proxyHost; //
  private final int proxyPort; //
  private Instant created; // UTC time for when record was created

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */
  public TransferProtocol(int id1, Mechanism mechanism1, int port1, boolean useProxy1, String proxyHost1, int proxyPort1, Instant created1)
  {
    id = id1;
    mechanism = mechanism1;
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
  public String getMechanism() { return mechanism.toString(); }
  public int getPort() { return port; }
  public boolean useProxy() { return useProxy; }
  public String getProxyHost() { return proxyHost; }
  public int getProxyPort() { return proxyPort; }
  @Schema(type = "string")
  public Instant getCreated() { return created; }

  @Override
  public String toString() {return TapisUtils.toString(this);}
}
