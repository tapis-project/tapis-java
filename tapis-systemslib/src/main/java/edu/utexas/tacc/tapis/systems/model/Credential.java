package edu.utexas.tacc.tapis.systems.model;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import org.apache.commons.lang3.StringUtils;

/*
 * Credential class representing an access credential stored in the Security Kernel.
 * Credentials are not persisted by the Systems Service. Actual secrets are managed by
 *   the Security Kernel.
 * The secret information will depend on the system type and access method.
 *
 * Immutable
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
  // String used to mask secrets
  public static final String SECRETS_MASK = "***";

  // Keys for constructing map when writing secrets to Security Kernel
  public static final String SK_KEY_PASSWORD = "password";
  public static final String SK_KEY_PUBLIC_KEY = "publicKey";
  public static final String SK_KEY_PRIVATE_KEY = "privateKey";
  public static final String SK_KEY_ACCESS_KEY = "accessKey";
  public static final String SK_KEY_ACCESS_SECRET = "accessSecret";


  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */

  // NOTE: In order to use jersey's SelectableEntityFilteringFeature fields cannot be final.
  private String password; // Password for when accessMethod is PASSWORD
  private String privateKey; // Private key for when accessMethod is PKI_KEYS or CERT
  private String publicKey; // Public key for when accessMethod is PKI_KEYS or CERT
  private String accessKey; // Access key for when accessMethod is ACCESS_KEY
  private String accessSecret; // Access secret for when accessMethod is ACCESS_KEY
  private String certificate; // SSH certificate for accessMethod is CERT

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */
  // Zero arg constructor needed to use jersey's SelectableEntityFilteringFeature
  public Credential() { }

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
  /*                        Public methods                                  */
  /* ********************************************************************** */
  /**
   * Create a credential with secrets masked out
   */
  public static Credential createMaskedCredential(Credential credential)
  {
    String accessKey, accessSecret, password, privateKey, publicKey, cert;
    accessKey = (!StringUtils.isBlank(credential.getAccessKey())) ? SECRETS_MASK : credential.getAccessKey();
    accessSecret = (!StringUtils.isBlank(credential.getAccessSecret())) ? SECRETS_MASK : credential.getAccessSecret();
    password = (!StringUtils.isBlank(credential.getPassword())) ? SECRETS_MASK : credential.getPassword();
    privateKey = (!StringUtils.isBlank(credential.getPrivateKey())) ? SECRETS_MASK : credential.getPrivateKey();
    publicKey = (!StringUtils.isBlank(credential.getPublicKey())) ? SECRETS_MASK : credential.getPublicKey();
    cert = (!StringUtils.isBlank(credential.getCertificate())) ? SECRETS_MASK : credential.getCertificate();
    return new Credential(password, privateKey, publicKey, accessKey, accessSecret, cert);
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
