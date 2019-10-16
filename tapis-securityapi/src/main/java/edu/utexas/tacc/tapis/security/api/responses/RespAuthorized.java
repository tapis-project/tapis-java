package edu.utexas.tacc.tapis.security.api.responses;

import edu.utexas.tacc.tapis.security.api.responseBody.BodyAuthorized;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespAuthorized
 extends RespAbstract
{
    public RespAuthorized(BodyAuthorized result) {this.result = result;}
    
    public BodyAuthorized result;
}
