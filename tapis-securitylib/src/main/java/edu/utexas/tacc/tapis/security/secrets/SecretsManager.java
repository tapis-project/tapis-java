package edu.utexas.tacc.tapis.security.secrets;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.response.AuthResponse;

import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class SecretsManager 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SecretsManager.class);
    
    // The secrets engine version we use by default.
    private static final int DEFAULT_SECRETS_ENGINE_VERSION = 2;
    
    // Filler address used when vault is disabled.
    private static final String DUMMY_VAULT_ADDR = "http://localhost";
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // Singleton instance of this class.
    private static SecretsManager _instance;
    
    // SK's vault token authentication response that contains its token.
    private AuthResponse _tokenAuth;
    
    // The configuration used to communicate with vault. 
    private VaultConfig  _vaultConfig;
    
    // Reusable vault instance.
    private Vault        _vault;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    private SecretsManager(ISecretsManagerParms parms)
     throws TapisRuntimeException
    {initInstance(parms);}
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getInstance:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Get the singleton instance of this class, creating it if necessary.
     * 
     * @param roleId the SK role id assigned by vault
     * @param secretId the short-lived approle secret assigned by vault
     * @return the singleton instance
     */
    public static synchronized SecretsManager getInstance(ISecretsManagerParms parms)
     throws TapisRuntimeException
    {
        // Create the instance if necessary.
        if (_instance == null) 
            _instance = new SecretsManager(parms);
        
        return _instance;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getInstance:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Return the non-null singleton instance or throw an exception if it 
     * doesn't exist.
     * 
     * @return the singleton instance
     * @throws TapisRuntimeException when the singleton does not exist
     */
    public static SecretsManager getInstance()  
     throws TapisRuntimeException
    {
        return getInstance(false);
    }
    
    /* ---------------------------------------------------------------------- */
    /* getInstance:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Return the singleton instance with custom exception handling.  If the
     * boolean parameter is true, then a null singleton can be returned. 
     * Otherwise, a runtime execption is thrown.  
     * 
     * @param allowNullResult true allows a null return instead of an exception
     *           when the singleton doesn't exist.
     * @return the singleton 
     * @throws TapisRuntimeException when the singleton doesn't exist and 
     *           allowNullResult is false
     */
    public static SecretsManager getInstance(boolean allowNullResult)
     throws TapisRuntimeException
    {
        // Make sure we have an initialed instance.
        if (!allowNullResult && _instance == null) {
            String msg = MsgUtils.getMsg("SK_NO_SECRETS_CONTEXT");
            throw new TapisRuntimeException(msg);
        }
        return _instance;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getSkToken:                                                            */
    /* ---------------------------------------------------------------------- */
    public String getSkToken() 
    {
        if (_tokenAuth == null) return null;
        return _tokenAuth.getAuthClientToken();
    }
    
    /* ---------------------------------------------------------------------- */
    /* getVault:                                                              */
    /* ---------------------------------------------------------------------- */
    public Vault getVault(){return _vault;}
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* initInstance:                                                          */
    /* ---------------------------------------------------------------------- */
    /** The method will initialize communication and login to Vault.  The login
     * will acquire the SK token that is used on all subsequent Vault calls.
     * 
     * Errors during initialization cause a runtime exception to be thrown.  We
     * need to be careful not to repeatedly try to restart the SK container
     * using the same invalid configuration parameters.
     * 
     * @param parms configuration parameters object
     * @throws TapisRuntimeException on error
     */
    private void initInstance(ISecretsManagerParms parms)
     throws TapisRuntimeException
    {
        // Use our custom loader to cut off any automatic environment loading by the driver.
        var loader = new SecretsNoOpLoader();
        
        // -------------------------- Vault Disabled --------------------------
        // Handle the simple case in which vault disabled.
        if (parms.isVaultDisabled()) {
            try {_vaultConfig = new VaultConfig().
                                    environmentLoader(loader).
                                    address(DUMMY_VAULT_ADDR).
                                    build();}
            catch (VaultException e) {
                String msg = MsgUtils.getMsg("SK_VAULT_CONFIG_ERROR", e.getMessage());
                _log.error(msg, e);
                throw new TapisRuntimeException(msg, e);
            }
            return;
        }
        
        // --------------------------- Vault Enabled ----------------------------
        // Determine if we are using SSL communication to the vault server.
        SkSslConfig sslConfig;
        try {
            // TODO: Finish configuring for https authentication.  This currently will not work.
            if (parms.isVaultSslVerify()) {
                sslConfig = (SkSslConfig) new SkSslConfig().verify(true).build();
            } 
            else 
                sslConfig = (SkSslConfig) new SkSslConfig().verify(false).build();
        } catch (Exception e) {
            String msg = MsgUtils.getMsg("SK_VAULT_SSL_CONFIG_ERROR", e.getMessage());
            _log.error(msg, e);
            throw new TapisRuntimeException(msg, e);
        }
        
        // Initialize the vault configuration using the inputed parms.
        try {
            _vaultConfig = new VaultConfig().
                            environmentLoader(loader).
                            address(parms.getVaultAddress()).
                            engineVersion(DEFAULT_SECRETS_ENGINE_VERSION).
                            openTimeout(parms.getVaultOpenTimeout()).
                            readTimeout(parms.getVaultReadTimeout()).
                            sslConfig(sslConfig).
                            build();
        } catch (Exception e) {
            String msg = MsgUtils.getMsg("SK_VAULT_CONFIG_ERROR", e.getMessage());
            _log.error(msg, e);
            throw new TapisRuntimeException(msg, e);
        }
        
        // Login using the role and secret ids, saving the auth response.
        // The auth response contains token information useful for renewal.
        try {
            _vault = new Vault(_vaultConfig);
            _tokenAuth = _vault.auth().loginByAppRole(parms.getVaultRoleId(), parms.getVaultSecretId());
        } catch (Exception e) {
            String msg = MsgUtils.getMsg("SK_VAULT_APPROLE_LOGIN_FAILED", e.getMessage());
            _log.error(msg, e);
            throw new TapisRuntimeException(msg, e);
        }
        
        // Add the token to the configuration.
        _vaultConfig.token(_tokenAuth.getAuthClientToken());
        
        // Print an informational blurb.
        String policies = StringUtils.join(_tokenAuth.getAuthPolicies(), ", ");
        _log.info(MsgUtils.getMsg("SK_VAULT_APPROLE_TOKEN_ACQUIRED", _tokenAuth.isAuthRenewable(), _tokenAuth.getAuthLeaseDuration(), policies));
    }
}
