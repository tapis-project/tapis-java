package edu.utexas.tacc.tapis.security.commands.model;

/** Base class for all secrets that maintains outcomes 
 * as each secret is processed.
 * 
 * @author rcardone
 */
public abstract class SkAdminAbstractSecret 
{
    public boolean failed;
    public String  tenant;
    public String  kubeSecretName;
    public String  kubeSecretKey;
}
