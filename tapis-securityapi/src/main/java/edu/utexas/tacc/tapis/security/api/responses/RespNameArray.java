package edu.utexas.tacc.tapis.security.api.responses;

import edu.utexas.tacc.tapis.security.api.responseBody.BodyNameArray;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespNameArray
 extends RespAbstract
{
    public RespNameArray(BodyNameArray result) {this.result = result;}
    
    public BodyNameArray result;
}
