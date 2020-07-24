package edu.utexas.tacc.tapis.security.commands.model;

import java.util.HashMap;
import java.util.Map;

import edu.utexas.tacc.tapis.security.secrets.SecretPathMapper.SecretPathMapperParms;
import edu.utexas.tacc.tapis.security.commands.processors.SkAdminAbstractProcessor;
import edu.utexas.tacc.tapis.security.secrets.SecretType;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;

public final class SkAdminServicePwd
 extends SkAdminAbstractSecret
{
    public String secretName;
    public String password;
    
    @Override
    public edu.utexas.tacc.tapis.security.client.model.SecretType 
    getClientSecretType() {return edu.utexas.tacc.tapis.security.client.model.SecretType.ServicePwd;}
    
    @Override
    public SecretType getSecretType() {return SecretType.ServicePwd;}

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
        // Add the password into the map field. We hardcode
        // the key as "password" in a single element map that
        // gets saved as the actual secret map in vault. 
        var map = new HashMap<String,Object>();
        map.put(SkAdminAbstractProcessor.DEFAULT_KEY_NAME, password);
        return map;
    }
}
