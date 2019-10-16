package edu.utexas.tacc.tapis.security.api.responses;

import edu.utexas.tacc.tapis.security.api.responseBody.BodyResourceUrl;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespResourceUrl
 extends RespAbstract
{
    public RespResourceUrl(BodyResourceUrl result) {this.result = result;}
    
    public BodyResourceUrl result;
}
