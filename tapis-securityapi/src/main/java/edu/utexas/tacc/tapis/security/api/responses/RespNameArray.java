package edu.utexas.tacc.tapis.security.api.responses;

import edu.utexas.tacc.tapis.security.api.responses.results.ResultNameArray;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespNameArray
 extends RespAbstract
{
    public RespNameArray(ResultNameArray result) {this.result = result;}
    
    public ResultNameArray result;
}
