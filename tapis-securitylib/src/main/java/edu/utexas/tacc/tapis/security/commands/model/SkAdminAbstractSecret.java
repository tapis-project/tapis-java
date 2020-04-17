package edu.utexas.tacc.tapis.security.commands.model;

import java.util.Map;

import edu.utexas.tacc.tapis.security.secrets.SecretPathMapper.SecretPathMapperParms;
import edu.utexas.tacc.tapis.security.secrets.SecretType;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;

/** Base class for all secrets that maintains outcomes 
 * as each secret is processed.
 * 
 * @author rcardone
 */
public abstract class SkAdminAbstractSecret 
{
    public boolean failed;
    public String  tenant;
    public String  user;
    public String  kubeSecretName;
    public String  kubeSecretKey;
    
    // Get either the client generated or securitylib SecretType.
    public abstract edu.utexas.tacc.tapis.security.client.model.SecretType getClientSecretType();
    public abstract SecretType getSecretType();
    
    // Get the path parameters customized for the concrete secret type.
    public abstract SecretPathMapperParms getSecretPathParms() throws TapisImplException;
    
    // Create the secret map for each secret type.
    public abstract Map<String,Object> getSecretMap();
}
