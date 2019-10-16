package edu.utexas.tacc.tapis.security.api.responses;

import edu.utexas.tacc.tapis.security.api.responseBody.BodyChangeCount;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespChangeCount
 extends RespAbstract
{
    public RespChangeCount(BodyChangeCount result) {this.result = result;}
    
    public BodyChangeCount result;
}
