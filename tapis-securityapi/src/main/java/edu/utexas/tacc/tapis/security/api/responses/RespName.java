package edu.utexas.tacc.tapis.security.api.responses;

import edu.utexas.tacc.tapis.security.api.responses.results.ResultName;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespName
 extends RespAbstract
{
    public RespName(ResultName result) {this.result = result;}
    
    public ResultName result;
}
