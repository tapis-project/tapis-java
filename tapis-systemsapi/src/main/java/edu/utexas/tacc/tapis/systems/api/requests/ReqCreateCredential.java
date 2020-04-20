package edu.utexas.tacc.tapis.systems.api.requests;

/*
 * Class representing Credential attributes that can be set in an incoming create request json body
 */
public final class ReqCreateCredential
{
  public String password; // Password for when accessMethod is PASSWORD
  public String privateKey; // Private key for when accessMethod is PKI_KEYS or CERT
  public String publicKey; // Public key for when accessMethod is PKI_KEYS or CERT
  public String accessKey; // Access key for when accessMethod is ACCESS_KEY
  public String accessSecret; // Access secret for when accessMethod is ACCESS_KEY
  public String certificate; // SSH certificate for accessMethod is CERT
}
