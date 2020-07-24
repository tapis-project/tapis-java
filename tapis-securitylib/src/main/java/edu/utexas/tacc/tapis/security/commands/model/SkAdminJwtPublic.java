package edu.utexas.tacc.tapis.security.commands.model;

import edu.utexas.tacc.tapis.security.secrets.SecretType;

import java.util.Map;

import edu.utexas.tacc.tapis.security.secrets.SecretPathMapper.SecretPathMapperParms;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;

public final class SkAdminJwtPublic 
 extends SkAdminAbstractSecret
{
    public String secretName;
    public String publicKey;
    
    @Override
    public edu.utexas.tacc.tapis.security.client.model.SecretType 
    getClientSecretType() {return edu.utexas.tacc.tapis.security.client.model.SecretType.JWTSigning;}
    
    @Override
    public SecretType getSecretType() {return SecretType.JWTSigning;}

    // This method is used on kube deployments to read from vault for kube deployment.
    @Override
    public SecretPathMapperParms getSecretPathParms() 
      throws TapisImplException 
    {
        // Create the parm container object.
        SecretPathMapperParms parms = new SecretPathMapperParms(getSecretType());
        
        // Translate the "+" sign into slashes to allow for vault subdirectories.
        if (secretName != null) secretName = secretName.replace('+', '/');
        
        // Assign the rest of the parm fields.
        parms.setSecretName(secretName);
        
        return parms;
    }

    // This method will never be called because this class is only used on kube 
    // deployments and this method is only used for vault updates.
    @Override
    public Map<String, Object> getSecretMap() {return null;}
}
