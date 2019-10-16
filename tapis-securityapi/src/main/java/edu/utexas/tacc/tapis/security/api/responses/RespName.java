package edu.utexas.tacc.tapis.security.api.responses;

import edu.utexas.tacc.tapis.security.api.responseBody.BodyName;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespName
 extends RespAbstract
{
    public RespName(BodyName result) {this.result = result;}
    
    public BodyName result;
}
