package edu.utexas.tacc.tapis.security.api.responses;

import edu.utexas.tacc.tapis.security.api.resources.SecurityResource.SkProbe;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespProbe 
 extends RespAbstract
{
    public RespProbe(SkProbe probe) {result = probe;}
    
    public SkProbe result;
}
