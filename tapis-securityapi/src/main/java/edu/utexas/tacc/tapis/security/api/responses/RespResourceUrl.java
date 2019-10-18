package edu.utexas.tacc.tapis.security.api.responses;

import edu.utexas.tacc.tapis.security.api.responses.results.ResultResourceUrl;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespResourceUrl
 extends RespAbstract
{
    public RespResourceUrl(ResultResourceUrl result) {this.result = result;}
    
    public ResultResourceUrl result;
}
