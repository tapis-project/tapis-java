package edu.utexas.tacc.tapis.security.commands.model;

import edu.utexas.tacc.tapis.security.secrets.SecretPathMapper.KeyType;

public final class SkAdminSystem
 extends SkAdminAbstractSecret
{
    
    public String  tenant;
    public String  system;
    public String  user;
    public KeyType keytype;
    public String  secretName;
    public String  secret;
    public String  kubeSecretName;
    
    // Convert to enum.
    public void setKeytype(String keytypeStr) {
        this.keytype = KeyType.valueOf(keytypeStr);
    }
}
