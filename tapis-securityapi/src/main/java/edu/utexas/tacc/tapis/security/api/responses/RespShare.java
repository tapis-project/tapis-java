package edu.utexas.tacc.tapis.security.api.responses;

import edu.utexas.tacc.tapis.security.authz.model.SkShare;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespShare
 extends RespAbstract
{
    public RespShare(SkShare share) {result = share;}
    
    public SkShare result;
}
