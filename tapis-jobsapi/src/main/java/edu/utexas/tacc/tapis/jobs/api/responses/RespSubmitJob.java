package edu.utexas.tacc.tapis.jobs.api.responses;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespSubmitJob 
 extends RespAbstract
{
    public RespSubmitJob(Job job) {result = job;}
    
    public Job result;
}
