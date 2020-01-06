package edu.utexas.tacc.tapis.security.api.responses;

import java.util.HashMap;
import java.util.Map;

import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespSecret 
 extends RespAbstract
{
    // Fields
    public Map<String,String> secretMap = new HashMap<String,String>();
    public SecretMetadata metadata;
    
    // Classes
    public static final class SecretMetadata {
        public String  created_time;
        public String  deletion_time;
        public boolean destroyed;
        public int     version;
    }
}
