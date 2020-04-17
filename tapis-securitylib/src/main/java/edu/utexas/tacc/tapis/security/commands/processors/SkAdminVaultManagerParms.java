package edu.utexas.tacc.tapis.security.commands.processors;

import edu.utexas.tacc.tapis.security.secrets.IVaultManagerParms;

public final class SkAdminVaultManagerParms 
  implements IVaultManagerParms 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Vault defaults.
    private static final int DEFAULT_VAULT_OPEN_TIMEOUT_SECS = 20;
    private static final int DEFAULT_VAULT_READ_TIMEOUT_SECS = 20;
     
    // Vault token renewal defaults which shouldn't ever come into play.
    private static final int DEFAULT_VAULT_TOKEN_SECONDS = 600;   // 10 minutes
    private static final int DEFAULT_VAULT_TOKEN_THRESHOLD = 80;  // percent 

    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // Vault parameters.
    private boolean vaultDisabled;        // disable vault processing
    private boolean vaultRenewalDisabled; // disable automatic token renewal
    private String  vaultAddress;         // vault server address
    private String  vaultRoleId;          // approle role id assigned to SK for logon
    private String  vaultSecretId;        // approle secret id for logon
    private int     vaultOpenTimeout;     // connection timeout in seconds
    private int     vaultReadTimeout;     // read response timeout in seconds
    private boolean vaultSslVerify;       // whether to use http or https
    private String  vaultSslCertFile;     // certificate file containing vault's public key
    private String  vaultSkKeyPemFile;    // PEM file containing SK's private key 
    private int     vaultRenewSeconds;    // expiration time in seconds of SK token
    private int     vaultRenewThreshold;  // point at which token renewal begins,
                                          //   expressed as percent of expiration time
    
    /* ********************************************************************** */
    /*                             Constructors                               */
    /* ********************************************************************** */
    public SkAdminVaultManagerParms()
    {
        // Set defaults.
        vaultRenewalDisabled = true;
        vaultOpenTimeout     = DEFAULT_VAULT_OPEN_TIMEOUT_SECS;
        vaultReadTimeout     = DEFAULT_VAULT_READ_TIMEOUT_SECS;
        vaultRenewSeconds    = DEFAULT_VAULT_TOKEN_SECONDS;
        vaultRenewThreshold  = DEFAULT_VAULT_TOKEN_THRESHOLD;
    }
    
    /* ********************************************************************** */
    /*                           Public Methods                               */
    /* ********************************************************************** */
    @Override
    public boolean isVaultDisabled() {return vaultDisabled;}
    
    @Override
    public boolean isVaultRenewalDisabled() {return vaultRenewalDisabled;}

    @Override
    public String getVaultAddress() {return vaultAddress;}

    @Override
    public String getVaultRoleId() {return vaultRoleId;}

    @Override
    public String getVaultSecretId() {return vaultSecretId;}

    @Override
    public int getVaultOpenTimeout() {return vaultOpenTimeout;}

    @Override
    public int getVaultReadTimeout() {return vaultReadTimeout;}

    @Override
    public boolean isVaultSslVerify() {return vaultSslVerify;}

    @Override
    public String getVaultSslCertFile() {return vaultSslCertFile;}

    @Override
    public String getVaultSkKeyPemFile() {return vaultSkKeyPemFile;}

    @Override
    public int getVaultRenewSeconds() {return vaultRenewSeconds;}

    @Override
    public int getVaultRenewThreshold() {return vaultRenewThreshold;}

    // Setters.
    public void setVaultDisabled(boolean vaultDisabled) {
        this.vaultDisabled = vaultDisabled;
    }

    public void setVaultAddress(String vaultAddress) {
        this.vaultAddress = vaultAddress;
    }

    public void setVaultRoleId(String vaultRoleId) {
        this.vaultRoleId = vaultRoleId;
    }

    public void setVaultSecretId(String vaultSecretId) {
        this.vaultSecretId = vaultSecretId;
    }

    public void setVaultOpenTimeout(int vaultOpenTimeout) {
        this.vaultOpenTimeout = vaultOpenTimeout;
    }

    public void setVaultReadTimeout(int vaultReadTimeout) {
        this.vaultReadTimeout = vaultReadTimeout;
    }

    public void setVaultSslVerify(boolean vaultSslVerify) {
        this.vaultSslVerify = vaultSslVerify;
    }

    public void setVaultSslCertFile(String vaultSslCertFile) {
        this.vaultSslCertFile = vaultSslCertFile;
    }

    public void setVaultSkKeyPemFile(String vaultSkKeyPemFile) {
        this.vaultSkKeyPemFile = vaultSkKeyPemFile;
    }

    public void setVaultRenewSeconds(int vaultRenewSeconds) {
        this.vaultRenewSeconds = vaultRenewSeconds;
    }

    public void setVaultRenewThreshold(int vaultRenewThreshold) {
        this.vaultRenewThreshold = vaultRenewThreshold;
    }
}
