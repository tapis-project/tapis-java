package edu.utexas.tacc.tapis.jobs.api.requestBody;

import java.util.List;

public class ReqShareJob 
 implements IReqBody
{
    /* **************************************************************************** */
    /*                                   Fields                                     */
    /* **************************************************************************** */
	// Fields
   
    private String   			grantee;
    private List<String>        jobResource ;
    private String              jobPermission;
    
    
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

	public List<String> getJobResource() {
		return jobResource;
	}

	public void setJobResource(List<String> jobResource) {
		this.jobResource = jobResource;
	}

	public String getJobPermission() {
		return jobPermission;
	}

	public void setJobPermission(String jobPermission) {
		this.jobPermission = jobPermission;
	}

}

