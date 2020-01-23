package edu.utexas.tacc.tapis.systems.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.systems.model.Protocol.AccessMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemType;

/*
 * Credential class representing an access credential stored in the Security Kernel.
 * Credentials are not persisted by the Systems Service. Actual secrets are managed by
 *   the Security Kernel.
 * Each credential will always include the non-secret information including:
 *   tenant, system, user, systemType, accessMethod
 * The secret information will depend on the system type and access method.
 *
 * This class is intended to represent an immutable object.
 * Please keep it immutable.
 *
 */
public final class Credential
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */

  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  // Logging
  private static final Logger _log = LoggerFactory.getLogger(Credential.class);

  private final String tenant; // Name of the tenant. Tenant + system + type + name must be unique.
  private final String system; // Name of the system that supports the capability
  private final String user; // Name of user
  private SystemType systemType; // Type of system, e.g. LINUX, OBJECT_STORE
  private final AccessMethod accessMethod; // How access authorization is handled
  private final char[] password; // Password for when accessMethod is PASSWORD
  private final char[] privateKey; // Private key for when accessMethod is PKI_KEYS or CERT
  private final char[] publicKey; // Public key for when accessMethod is PKI_KEYS or CERT
  private final char[] certificate; // SSH certificate for accessMethod is CERT
  private final char[] accessKey; // Access key for when accessMethod is ACCESS_KEY
  private final char[] accessSecret; // Access secret for when accessMethod is ACCESS_KEY

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */

  /**
   * Default constructor sets all attributes to null
   */
  public Credential()
  {
    tenant = null;
    system = null;
    user = null;
    systemType = null;
    accessMethod = null;
    password = null;
    privateKey = null;
    publicKey = null;
    certificate = null;
    accessKey = null;
    accessSecret = null;
  }

  /**
   * Simple constructor to populate all attributes
   */
  public Credential(String tenant1, String system1, String user1, SystemType systemType1, AccessMethod accessMethod1,
                    char[] password1, char[] privateKey1, char[] publicKey1, char[] cert1,
                    char[] accessKey1, char[] accessSecret1)
  {
    tenant = tenant1;
    system = system1;
    user = user1;
    systemType = systemType1;
    accessMethod = accessMethod1;
    password = password1;
    privateKey = privateKey1;
    publicKey = publicKey1;
    certificate = cert1;
    accessKey = accessKey1;
    accessSecret = accessSecret1;
  }

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
  public String getTenant() { return tenant; }
  public String getSystem() { return system; }
  public String getUser() { return user; }
  public SystemType getSystemType() { return systemType; }
  public AccessMethod getAccessMethod() { return accessMethod; }
  public char[] getPassword() { return password; }
  public char[] getPrivateKey() { return privateKey; }
  public char[] getPublicKey() { return publicKey; }
  public char[] getCertificate() { return certificate; }
  public char[] getAccessKey() { return accessKey; }
  public char[] getAccessSecret() { return accessSecret; }

  @Override
  public String toString() {return TapisUtils.toString(this);}
}
