package edu.utexas.tacc.tapis.security.commands.model;

import java.util.HashMap;
import java.util.Map;

import edu.utexas.tacc.tapis.security.commands.processors.SkAdminAbstractProcessor;
import edu.utexas.tacc.tapis.security.secrets.SecretPathMapper.SecretPathMapperParms;
import edu.utexas.tacc.tapis.security.secrets.SecretType;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;

public final class SkAdminDBCredential 
 extends SkAdminAbstractSecret
{
    public String dbservice;
    public String dbhost;
    public String dbname;
    public String secretName;
    public String secret;
    
    @Override
    public edu.utexas.tacc.tapis.security.client.model.SecretType 
    getClientSecretType() {return edu.utexas.tacc.tapis.security.client.model.SecretType.DBCredential;}
    
    @Override
    public SecretType getSecretType() {return SecretType.DBCredential;}

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
        parms.setDbHost(dbhost);
        parms.setDbName(dbname);
        parms.setDbService(dbservice);
        
        return parms;
    }

    @Override
    public Map<String, Object> getSecretMap() 
    {
        // Add the password into the map field. We hardcode
        // the key as "password" in a single element map that
        // gets saved as the actual secret map in vault. 
        var secretMap = new HashMap<String,Object>();
        secretMap.put(SkAdminAbstractProcessor.DEFAULT_KEY_NAME, secret);
        return secretMap;
    }
}
