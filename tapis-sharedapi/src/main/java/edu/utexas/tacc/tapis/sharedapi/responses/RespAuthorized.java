package edu.utexas.tacc.tapis.sharedapi.responses;

import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultAuthorized;

public final class RespAuthorized
 extends RespAbstract
{
    public RespAuthorized(ResultAuthorized result) {this.result = result;}
    
    public ResultAuthorized result;
}
