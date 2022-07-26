package edu.utexas.tacc.tapis.jobs.api.requestBody;

import java.util.List;

import edu.utexas.tacc.tapis.jobs.model.enumerations.JobResourceShare;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobTapisPermission;

public class ReqShareJob 
 implements IReqBody
{
    /* **************************************************************************** */
    /*                                   Fields                                     */
    /* **************************************************************************** */
	// Fields
   
    private String   			grantee;
    private List<JobResourceShare> jobResource;
    private JobTapisPermission     jobPermission;
    
    
	@Override
	public String validate() 
	{
		// Success.
		return null; // json schema validation is sufficient
	}
	
	

	public String getGrantee() {
		return grantee;
	}

	public void setGrantee(String grantee) {
		this.grantee = grantee;
	}

	public List<JobResourceShare> getJobResource() {
		return jobResource;
	}

	public void setJobResource(List<JobResourceShare> jobResource) {
		this.jobResource = jobResource;
	}
	public JobTapisPermission getJobPermission() {
		return jobPermission;
	}

	public void setJobPermission(JobTapisPermission jobPermission) {
		this.jobPermission = jobPermission;
	}

}

