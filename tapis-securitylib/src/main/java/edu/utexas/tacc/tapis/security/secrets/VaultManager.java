package edu.utexas.tacc.tapis.security.secrets;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.response.AuthResponse;

import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class VaultManager
  implements Thread.UncaughtExceptionHandler
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(VaultManager.class);
    
    // The secrets engine version we use by default.
    private static final int DEFAULT_SECRETS_ENGINE_VERSION = 2;
    
    // Filler address used when vault is disabled.
    private static final String DUMMY_VAULT_ADDR = "http://localhost";
    
    // Renewal thread info.
    private static final String THREADGROUP_NAME  = "SkTokRenewalGroup";
    private static final String RENEW_THREAD_NAME = "SkTokRenewalThread";
    private static final int MAX_RENEWAL_ATTEMPTS = 10;
    
    // HTTP status codes.
    private static final int HTTP_FORBIDDEN = 403;
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // Singleton instance of this class.
    private static VaultManager _instance;
    
    // Save the parms object.
    private final IVaultManagerParms _parms;
    
    // SK's vault token authentication response that contains its token.
    private AuthResponse _tokenAuth;
    
    // The configuration used to communicate with vault. 
    private VaultConfig  _vaultConfig;
    
    // Reusable vault instance.
    private Vault        _vault;
    
    // Start off health and change if something goes wrong.
    private volatile boolean _healthy = true;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    private VaultManager(IVaultManagerParms parms)
     throws TapisRuntimeException
    {
        // Save the parms object before initialization.
        _parms = parms;
        initInstance();
    }
    
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
    public static synchronized VaultManager getInstance(IVaultManagerParms parms)
     throws TapisRuntimeException
    {
        // Create the instance if necessary.
        if (_instance == null) 
            _instance = new VaultManager(parms);
        
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
    public static VaultManager getInstance()  
     throws TapisRuntimeException
    {
        return getInstance(false);
    }
    
    /* ---------------------------------------------------------------------- */
    /* getInstance:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Return the singleton instance with custom exception handling.  If the
     * boolean parameter is true, then a null singleton can be returned. 
     * Otherwise, a runtime exception is thrown.  
     * 
     * @param allowNullResult true allows a null return instead of an exception
     *           when the singleton doesn't exist.
     * @return the singleton 
     * @throws TapisRuntimeException when the singleton doesn't exist and 
     *           allowNullResult is false
     */
    public static VaultManager getInstance(boolean allowNullResult)
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
    
    /* ---------------------------------------------------------------------- */
    /* isReady:                                                               */
    /* ---------------------------------------------------------------------- */
    /** Return true only if we have disabled secrets or, if secrets are enabled,
     * are healthy and we have an token. We use the existence of _tokenAuth
     * as a substitute for actually checking the _vaultConfig for a token 
     * because it's cheaper and essentially as accurate. 
     * */
    public boolean isReady()
    {
        return _parms.isVaultDisabled() || (_healthy && (_tokenAuth != null));
    }
    
    /* ---------------------------------------------------------------------- */
    /* isHealthy:                                                             */
    /* ---------------------------------------------------------------------- */
    /** Return true if we have disabled secrets or, if secrets are enabled, 
     * that the healthy flag is set.  
     */
    public boolean isHealthy(){return _parms.isVaultDisabled() || _healthy;}
    
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
     * @throws TapisRuntimeException on error
     */
    private void initInstance()
     throws TapisRuntimeException
    {
        // Use our custom loader to cut off any automatic environment loading by the driver.
        var loader = new VaultNoOpLoader();
        
        // -------------------------- Vault Disabled --------------------------
        // Handle the simple case in which vault disabled.
        if (_parms.isVaultDisabled()) {
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
            if (_parms.isVaultSslVerify()) {
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
                            address(_parms.getVaultAddress()).
                            engineVersion(DEFAULT_SECRETS_ENGINE_VERSION).
                            openTimeout(_parms.getVaultOpenTimeout()).
                            readTimeout(_parms.getVaultReadTimeout()).
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
            _tokenAuth = _vault.auth().loginByAppRole(_parms.getVaultRoleId(), _parms.getVaultSecretId());
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
    
        // Start the token renewal thread.
        startTokenRenewalThread();
    }
    
    /* ---------------------------------------------------------------------- */
    /* startTokenRenewalThread:                                               */
    /* ---------------------------------------------------------------------- */
    private void startTokenRenewalThread()
    {
        // Create and start the daemon thread only AFTER token is first acquired.
        var threadGroup = new ThreadGroup(THREADGROUP_NAME);
        var renewalThread = new TokenRenewalThread(threadGroup, RENEW_THREAD_NAME);
        renewalThread.setDaemon(true);
        renewalThread.setUncaughtExceptionHandler(this);
        renewalThread.start();
    }

    /* ---------------------------------------------------------------------- */
    /* uncaughtException:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Note the unexpected death of our renewal thread.  We just let it die
     * and wait for the token to eventually expire, which will cause our health 
     * monitoring to take corrective action.
     */
    @Override
    public void uncaughtException(Thread t, Throwable e) 
    {
        // Record the error.
        _log.error(MsgUtils.getMsg("TAPIS_THREAD_UNCAUGHT_EXCEPTION", 
                                   t.getName(), e.toString()));
        e.printStackTrace(); // stderr for emphasis
    }
    
    /* ********************************************************************** */
    /*                         TokenRenewalThread Class                       */
    /* ********************************************************************** */
    /** This inner class will renew the SK token as indicated by the applicable
     * input parameters.  The enclosing class's token response field is atomically
     * updated upon successful token renewal.
     */
    private final class TokenRenewalThread
     extends Thread
    {
        /* ---------------------------------------------------------------------- */
        /* constructor:                                                           */
        /* ---------------------------------------------------------------------- */
        private TokenRenewalThread(ThreadGroup threadGroup, String threadName) 
        {
            super(threadGroup, threadName);
        }
        
        /* ---------------------------------------------------------------------- */
        /* run:                                                                   */
        /* ---------------------------------------------------------------------- */
        @Override
        public void run() 
        {
            // No token renew is needed if our token has root 
            // privileges since it never expires.
            List<String> policies = _tokenAuth.getAuthPolicies();
            if (policies != null && policies.contains("root")) return;
            
            // Get the ttl that we request on token renewal.
            final long renewSeconds = _parms.getVaultRenewSeconds();
            
            // Renewal processing loop.
            int  attempt = 1; // minimum attempt value
            long initialSleepSecs = 0;
            long retrySleepSecs = 0;
            while (true) {
                // Have we run out of renewal attempts?
                if (attempt > MAX_RENEWAL_ATTEMPTS) {
                    // Indicate that we are giving up trying to renew this token.
                    String msg = MsgUtils.getMsg("SK_VAULT_TOKEN_MAX_RENEWALS", 
                                    Thread.currentThread().getName(), MAX_RENEWAL_ATTEMPTS);
                    _log.error(msg);

                    // Advertise our problem and terminate thread.
                    _healthy = false;
                    return;
                }
                
                // Calculate the initial and subsequent sleep time in seconds.
                // Note that on attempt 1 the token has been just been acquired 
                // or renewed, so we need to recalculate durations.
                if (attempt == 1) {
                    initialSleepSecs = calculateInitialSleepSeconds();
                    retrySleepSecs   = calculateRetrySleepSeconds(initialSleepSecs);
                }
                
                // Get the number of seconds to wait before the next renewal attempt.
                long sleepSecs = attempt == 1 ? initialSleepSecs : retrySleepSecs;
                if (_log.isDebugEnabled()) {
                    String msg = MsgUtils.getMsg("SK_VAULT_RENEWAL_THREAD_SLEEP",
                                                 Thread.currentThread().getName(), 
                                                 attempt, MAX_RENEWAL_ATTEMPTS,
                                                 sleepSecs, _tokenAuth.getAuthLeaseDuration());
                    _log.debug(msg);
                }
                
                // Wait until the next attempt should be tried.
                try {Thread.sleep(sleepSecs * 1000);}
                catch (InterruptedException e) {
                    String msg = MsgUtils.getMsg("SK_VAULT_RENEWAL_THREAD_INTERRUPTED",
                                                 Thread.currentThread().getName(), e.getMessage());
                    _log.warn(msg);
                    return;
                }
                
                // Let's renew the token.
                try {_tokenAuth = _vault.auth().renewSelf(renewSeconds);}
                catch (VaultException e) {
                    // When a token expires, we get a Forbidden status code back
                    // from Vault. We cannot recover from an expired token, so there's
                    // nothing left to do other than indicate that we are broken.
                    if (e.getHttpStatusCode() == HTTP_FORBIDDEN) {
                        String msg = MsgUtils.getMsg("SK_VAULT_INVALID_TOKEN", e.getMessage());
                        _log.error(msg, e);
       
                        // Advertise our problem and terminate thread.
                        _healthy = false;
                        return;
                    }
                    
                    // Some sort of vault error, we'll keep trying.
                    String msg = MsgUtils.getMsg("SK_VAULT_ERROR", "renewSelf", 
                                                 e.getHttpStatusCode(), e.getMessage());
                    _log.error(msg, e);
                    attempt++;
                    continue;
                }
                catch (Exception e) {
                    // General error, we'll keep trying.
                    String msg = MsgUtils.getMsg("SK_VAULT_TOKEN_RENEWAL_ERROR", attempt, 
                                                 MAX_RENEWAL_ATTEMPTS, e.getMessage());
                    _log.error(msg, e);
                    attempt++;
                    continue;
                }
                
                // ---------------------- Token Renewed ----------------------
                // Reset the attempt number back to 1. This forces sleep times
                // to be recalculated using the latest lease information.
                if (_log.isInfoEnabled()) {
                    String msg = MsgUtils.getMsg("SK_VAULT_TOKEN_RENEWED",
                                                 _tokenAuth.getAuthLeaseDuration(),
                                                 attempt, MAX_RENEWAL_ATTEMPTS);
                    _log.info(msg);
                }
                attempt = 1;
            }
        }
        
        /* ---------------------------------------------------------------------- */
        /* calculateInitialSleepSeconds:                                          */
        /* ---------------------------------------------------------------------- */
        /** Calculate the number of seconds to sleep before the first attempt to 
         * renew the token. We take the actual token duration from the token 
         * creation response, multiple it by the threshold and then divide by 100.
         * Overflow is unlikely given the use of longs.
         * 
         * @return the seconds to sleep
         */
        private long calculateInitialSleepSeconds() 
        {
            return (_tokenAuth.getAuthLeaseDuration() * _parms.getVaultRenewThreshold()) / 100L;
        }
        
        /* ---------------------------------------------------------------------- */
        /* calculateRetrySleepSeconds:                                            */
        /* ---------------------------------------------------------------------- */
        /** Calculate the sleep time for token renewal after the first attempt to 
         * renew the token fails. The approach is to divvy up the time remaining
         * after the first attempt into equal periods based on the approximate
         * number of possible retries (we ignore processing overhead time, so the
         * token may be expired by the time last attempt is made). 
         * 
         * @param initialSleepSecs the number of seconds before the 1st renewal attempt
         * @return the time to sleep on renewal attempts after the 1st attempt
         */
        private long calculateRetrySleepSeconds(long initialSleepSecs)
        {
            return (_tokenAuth.getAuthLeaseDuration() - initialSleepSecs) / MAX_RENEWAL_ATTEMPTS;
        }
    }
 }
