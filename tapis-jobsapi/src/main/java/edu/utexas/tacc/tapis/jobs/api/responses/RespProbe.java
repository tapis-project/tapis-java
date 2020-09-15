package edu.utexas.tacc.tapis.jobs.api.responses;

import edu.utexas.tacc.tapis.jobs.api.resources.GeneralResource.JobsProbe;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespProbe 
 extends RespAbstract
{
    public RespProbe(JobsProbe probe) {result = probe;}
    
    public JobsProbe result;
}
