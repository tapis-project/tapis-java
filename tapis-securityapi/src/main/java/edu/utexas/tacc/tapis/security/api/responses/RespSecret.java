package edu.utexas.tacc.tapis.security.api.responses;

import edu.utexas.tacc.tapis.security.authz.model.SkSecret;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespSecret 
 extends RespAbstract
{
    public RespSecret(SkSecret secret) {result = secret;}
    
    public SkSecret result;
}
