package edu.utexas.tacc.tapis.jobs.api.responses;

import edu.utexas.tacc.tapis.jobs.api.requestBody.ReqSubmitJob;

import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;


public final class RespGetResubmit 
extends RespAbstract
{
   public RespGetResubmit (ReqSubmitJob job) {result = job;}
   
   public ReqSubmitJob result;
}