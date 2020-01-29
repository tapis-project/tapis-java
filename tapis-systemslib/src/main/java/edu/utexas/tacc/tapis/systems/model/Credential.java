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

  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  // Logging
  private static final Logger _log = LoggerFactory.getLogger(Credential.class);

  private final String password; // Password for when accessMethod is PASSWORD
  private final String privateKey; // Private key for when accessMethod is PKI_KEYS or CERT
  private final String publicKey; // Public key for when accessMethod is PKI_KEYS or CERT
  private final String certificate; // SSH certificate for accessMethod is CERT
  private final String accessKey; // Access key for when accessMethod is ACCESS_KEY
  private final String accessSecret; // Access secret for when accessMethod is ACCESS_KEY

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */

  /**
   * Default constructor sets all attributes to null
   */
  public Credential()
  {
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
  public Credential(String password1, String privateKey1, String publicKey1, String cert1, String accessKey1, String accessSecret1)
  {
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
  public String getPassword() { return password; }
  public String getPrivateKey() { return privateKey; }
  public String getPublicKey() { return publicKey; }
  public String getCertificate() { return certificate; }
  public String getAccessKey() { return accessKey; }
  public String getAccessSecret() { return accessSecret; }

  @Override
  public String toString() {return TapisUtils.toString(this);}
}