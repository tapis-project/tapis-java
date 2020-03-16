package edu.utexas.tacc.tapis.systems.model;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.systems.model.TSystem.AccessMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO/TBD: Remove this class? not needed? might be from early dev work
/*
 * Credential class representing an access credential provided when creating a TSystem.
 * In this case the credential is for a static effectiveUserId and
 *   the systemType, stored in the Security Kernel.
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
public final class SystemCredential
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */

  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  // Logging
  private static final Logger _log = LoggerFactory.getLogger(SystemCredential.class);

  private final String tenant; // Name of the tenant. Tenant + system + type + name must be unique.
  private final String system; // Name of the system that supports the capability
  private final String user; // Name of user
  private SystemType systemType; // Type of system, e.g. LINUX, OBJECT_STORE
  private final AccessMethod accessMethod; // How access authorization is handled
  private final String password; // Password for when accessMethod is PASSWORD
  private final String privateKey; // Private key for when accessMethod is PKI_KEYS or CERT
  private final String publicKey; // Public key for when accessMethod is PKI_KEYS or CERT
  private final String accessKey; // Access key for when accessMethod is ACCESS_KEY
  private final String accessSecret; // Access secret for when accessMethod is ACCESS_KEY
  private final String certificate; // SSH certificate for when accessMethod is CERT

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */

  /**
   * Default constructor sets all attributes to null
   */
  public SystemCredential()
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
  public SystemCredential(String tenant1, String system1, String user1, SystemType systemType1, AccessMethod accessMethod1,
                          String password1, String privateKey1, String publicKey1, String cert1,
                          String accessKey1, String accessSecret1)
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
  public String getPassword() { return password; }
  public String getPrivateKey() { return privateKey; }
  public String getPublicKey() { return publicKey; }
  public String getCertificate() { return certificate; }
  public String getAccessKey() { return accessKey; }
  public String getAccessSecret() { return accessSecret; }

  @Override
  public String toString() {return TapisUtils.toString(this);}
}
