package edu.utexas.tacc.tapis.security.commands.model;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.security.commands.processors.SkAdminAbstractProcessor;
import edu.utexas.tacc.tapis.security.secrets.SecretPathMapper.SecretPathMapperParms;
import edu.utexas.tacc.tapis.security.secrets.SecretType;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;

public final class SkAdminJwtSigning 
 extends SkAdminAbstractSecret
{
    public String secretName;
    public String privateKey;
    public String publicKey;
    
    @Override
    public edu.utexas.tacc.tapis.security.client.model.SecretType 
    getClientSecretType() {return edu.utexas.tacc.tapis.security.client.model.SecretType.JWTSigning;}
    
    @Override
    public SecretType getSecretType() {return SecretType.JWTSigning;}
    
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

    @Override
    public Map<String, Object> getSecretMap() 
    {
        // Add the keys into the map field. We hardcode
        // the key names in the map that gets saved as the 
        // actual secret map in vault. 
        var map = new HashMap<String,Object>();
        map.put(SkAdminAbstractProcessor.DEFAULT_PRIVATE_KEY_NAME, privateKey);
        if (!StringUtils.isBlank(publicKey))
            map.put(SkAdminAbstractProcessor.DEFAULT_PUBLIC_KEY_NAME, publicKey);
        return map;
    }
}
