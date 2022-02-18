package edu.utexas.tacc.tapis.jobs.api.requestBody;

public class ReqShareJob 
 implements IReqBody
{
    /* **************************************************************************** */
    /*                                   Fields                                     */
    /* **************************************************************************** */
	// Fields
   
    private String   			userSharedWith;
    private String           	jobResource ;
    private String              jobPermission;
    
    
	@Override
	public String validate() 
	{
		// Success.
		return null; // json schema validation is sufficient
	}
	
	

	public String getUserSharedWith() {
		return userSharedWith;
	}

	public void setUserSharedWith(String userSharedWith) {
		this.userSharedWith = userSharedWith;
	}

	public String getJobResource() {
		return jobResource;
	}

	public void setJobResource(String jobResource) {
		this.jobResource = jobResource;
	}

	public String getJobPermission() {
		return jobPermission;
	}

	public void setJobPermission(String jobPermission) {
		this.jobPermission = jobPermission;
	}

}

