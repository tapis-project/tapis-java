package edu.utexas.tacc.tapis.security.secrets;

/** Interface that provides access to vault configuration parameters.
 * 
 * @author rcardone
 */
public interface IVaultManagerParms 
{
    public boolean isVaultDisabled();
    public String  getVaultAddress();
    public String  getVaultRoleId();
    public String  getVaultSecretId();
    public int     getVaultOpenTimeout();
    public int     getVaultReadTimeout();
    public boolean isVaultSslVerify();
    public String  getVaultSslCertFile();
    public String  getVaultSkKeyPemFile();
    public int     getVaultRenewSeconds();
    public int     getVaultRenewThreshold();
}
