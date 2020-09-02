package edu.utexas.tacc.tapis.jobs.api.responses;

import edu.utexas.tacc.tapis.jobs.api.resources.JobsResource.SkProbe;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespProbe 
 extends RespAbstract
{
    public RespProbe(SkProbe probe) {result = probe;}
    
    public SkProbe result;
}
