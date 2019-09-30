package edu.utexas.tacc.tapis.systems.model;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/*
 * Access Protocol contains information required to access a system, except for user and secret.
 * This class is intended to represent an immutable object.
 * Please keep it immutable.
 */
public final class AccessProtocol
{
  private enum Mechanism {SSH_ANONYMOUS, SSH_PASSWORD, SSH_KEYS, SSH_CERT}

  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  // Logging
  private static final Logger _log = LoggerFactory.getLogger(AccessProtocol.class);

  private final Mechanism mechanism; // How access authorization is handled.
  private final int port; // Port number used to access a system.
  private final boolean useProxy; // Indicates if a system should be accessed through a proxy.
  private final String proxyHost; //
  private final int proxyPort; //
  private Instant created; // UTC time for when record was created

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */
  public AccessProtocol(Mechanism mechanism1, int port1, boolean useProxy1, String proxyHost1, int proxyPort1, Instant created1)
  {
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
