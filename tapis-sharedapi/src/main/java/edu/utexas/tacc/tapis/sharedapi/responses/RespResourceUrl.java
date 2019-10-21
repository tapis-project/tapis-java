package edu.utexas.tacc.tapis.sharedapi.responses;

import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultResourceUrl;

public final class RespResourceUrl
 extends RespAbstract
{
    public RespResourceUrl(ResultResourceUrl result) {this.result = result;}
    
    public ResultResourceUrl result;
}
