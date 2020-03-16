package edu.utexas.tacc.tapis.systems.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

/*
 * Credential class representing an access credential stored in the Security Kernel.
 * Credentials are not persisted by the Systems Service. Actual secrets are managed by
 *   the Security Kernel.
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

  // Top level name for storing system secrets
  public static final String TOP_LEVEL_SECRET_NAME = "S1";

  // Keys for constructing map when writing secrets to Security Kernel
  public static final String SK_KEY_PASSWORD = "password";
  public static final String SK_KEY_PUBLIC_KEY = "publicKey";
  public static final String SK_KEY_PRIVATE_KEY = "privateKey";
  public static final String SK_KEY_ACCESS_KEY = "accessKey";
  public static final String SK_KEY_ACCESS_SECRET = "accessSecret";


  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  // Logging
  private static final Logger _log = LoggerFactory.getLogger(Credential.class);

  private final String password; // Password for when accessMethod is PASSWORD
  private final String privateKey; // Private key for when accessMethod is PKI_KEYS or CERT
  private final String publicKey; // Public key for when accessMethod is PKI_KEYS or CERT
  private final String accessKey; // Access key for when accessMethod is ACCESS_KEY
  private final String accessSecret; // Access secret for when accessMethod is ACCESS_KEY
  private final String certificate; // SSH certificate for accessMethod is CERT

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */

  /**
   * Simple constructor to populate all attributes
   */
  public Credential(String password1, String privateKey1, String publicKey1, String accessKey1, String accessSecret1, String cert1)
  {
    password = password1;
    privateKey = privateKey1;
    publicKey = publicKey1;
    accessKey = accessKey1;
    accessSecret = accessSecret1;
    certificate = cert1;
  }

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
  public String getPassword() { return password; }
  public String getPrivateKey() { return privateKey; }
  public String getPublicKey() { return publicKey; }
  public String getAccessKey() { return accessKey; }
  public String getAccessSecret() { return accessSecret; }
  public String getCertificate() { return certificate; }

  @Override
  public String toString() {return TapisUtils.toString(this);}
}
