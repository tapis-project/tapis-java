package edu.utexas.tacc.tapis.sharedapi.responses;

import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultNameArray;

public final class RespNameArray
 extends RespAbstract
{
    public RespNameArray(ResultNameArray result) {this.result = result;}
    
    public ResultNameArray result;
}
