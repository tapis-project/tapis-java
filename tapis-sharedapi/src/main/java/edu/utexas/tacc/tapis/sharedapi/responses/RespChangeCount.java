package edu.utexas.tacc.tapis.sharedapi.responses;

import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultChangeCount;

public final class RespChangeCount
 extends RespAbstract
{
    public RespChangeCount(ResultChangeCount result) {this.result = result;}
    
    public ResultChangeCount result;
}
