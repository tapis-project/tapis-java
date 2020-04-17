package edu.utexas.tacc.tapis.security.commands.model;

import java.util.HashMap;
import java.util.Map;

import edu.utexas.tacc.tapis.security.secrets.SecretPathMapper.SecretPathMapperParms;
import edu.utexas.tacc.tapis.security.secrets.SecretType;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;

public final class SkAdminUser 
 extends SkAdminAbstractSecret
{
    public String secretName;
    public String key;
    public String value;
    
    @Override
    public edu.utexas.tacc.tapis.security.client.model.SecretType 
    getClientSecretType() {return edu.utexas.tacc.tapis.security.client.model.SecretType.User;}
    
    @Override
    public SecretType getSecretType() {return SecretType.User;}

    @Override
    public SecretPathMapperParms getSecretPathParms() throws TapisImplException 
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
        // Add the user's key/value into the map.
        var map = new HashMap<String,Object>();
        map.put(key, value);
        return map;
    }
}
