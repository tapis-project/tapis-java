package edu.utexas.tacc.tapis.security.secrets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bettercloud.vault.EnvironmentLoader;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** This class is a no-op replacement for the built-in environment variable
 * loading class that comes the Java Vault Driver.  Rather than have the driver
 * get its configuration settings on-demand from the environment, we explicitly
 * set the settings in the objects that would otherwise use the default loader.
 * 
 * Specifically, only the SslConfig and VaultConfig classes use the environment
 * loader to configure themselves.  In all cases, these two classes only 
 * consult the loader when they encounter a field that has not been explicitly
 * set.  Our approach is to ALWAYS set all necessary fields in SslConfig and 
 * VaultConfig objects so that on-demand loading is never needed.
 * 
 * @author rcardone
 */
public final class VaultNoOpLoader 
 extends EnvironmentLoader
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(VaultNoOpLoader.class);
    
    // Serialization nonsense.
    private static final long serialVersionUID = -3963065000239599240L;
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* loadVariable:                                                          */
    /* ---------------------------------------------------------------------- */
    @Override
    public String loadVariable(final String name)
    {   
        // Keep the log clean of expected calls from the driver's build method.
        if ("VAULT_TOKEN".contentEquals(name)) return null;
        
        // We don't expect this method to be called, 
        // so we issue a warning when it is.
        String msg = MsgUtils.getMsg("SK_VAULT_NOOP_LOADER_CALLED", name);
        _log.warn(msg);
        return null;
    }
}
