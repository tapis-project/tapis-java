package edu.utexas.tacc.tapis.security.api.responses;

import edu.utexas.tacc.tapis.security.api.responses.results.ResultChangeCount;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespChangeCount
 extends RespAbstract
{
    public RespChangeCount(ResultChangeCount result) {this.result = result;}
    
    public ResultChangeCount result;
}
