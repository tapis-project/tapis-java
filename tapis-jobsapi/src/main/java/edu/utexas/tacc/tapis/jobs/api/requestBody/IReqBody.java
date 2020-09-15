package edu.utexas.tacc.tapis.jobs.api.requestBody;

public interface IReqBody 
{
    /** Return a user-appropriate error message on failed validation
     *  and return null if validation succeeds.
     */ 
    public String validate();
}
