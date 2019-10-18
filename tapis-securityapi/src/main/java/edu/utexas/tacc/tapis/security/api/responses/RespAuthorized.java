package edu.utexas.tacc.tapis.security.api.responses;

import edu.utexas.tacc.tapis.security.api.responses.results.ResultAuthorized;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespAuthorized
 extends RespAbstract
{
    public RespAuthorized(ResultAuthorized result) {this.result = result;}
    
    public ResultAuthorized result;
}
