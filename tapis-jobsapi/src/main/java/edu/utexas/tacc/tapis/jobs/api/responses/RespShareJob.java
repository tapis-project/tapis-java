package edu.utexas.tacc.tapis.jobs.api.responses;

import edu.utexas.tacc.tapis.jobs.model.JobShared;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public class RespShareJob extends RespAbstract
{
    public JobShared result;
    public RespShareJob(JobShared jobShared) {result = jobShared;}
}
