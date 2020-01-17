package edu.utexas.tacc.tapis.security.secrets;

import com.bettercloud.vault.SslConfig;

/** This class extends the vault driver class to get access to its protected members.
 * 
 * @author rcardone
 */
public final class SkSslConfig 
 extends SslConfig
{
    // Construct all instances with our custom environment loader.
    public SkSslConfig()
    {
        environmentLoader(new VaultNoOpLoader());
    }
}
