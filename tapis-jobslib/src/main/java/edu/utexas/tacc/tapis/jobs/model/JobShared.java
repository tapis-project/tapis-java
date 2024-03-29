package edu.utexas.tacc.tapis.jobs.model;

import java.time.Instant;

import edu.utexas.tacc.tapis.jobs.model.enumerations.JobResourceShare;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobTapisPermission;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import io.swagger.v3.oas.annotations.media.Schema;

public final class JobShared
{
		
	// Fields
    private int      			id;
    private String   			tenant;
    private String   			createdby;
    private String   			jobUuid;
    private String   			grantee;
    private String              grantor;
    private JobResourceShare  	jobResource ;
    private JobTapisPermission  jobPermission;
    private Instant  			created;
   
      
    /* **************************************************************************** */
    /*                                 Constructors                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    public JobShared() {}
    
    public JobShared(String tenant, String createdby, String jobUuid, String grantee, String grantor,
		   JobResourceShare jobResource, JobTapisPermission  jobPermission )
   {
        this.tenant = tenant;
        this.createdby = createdby;
        this.jobUuid = jobUuid;
        this.grantee = grantee;
        this.grantor = grantor;
        this.jobResource = jobResource;
        this.jobPermission = jobPermission;
    	
  	    // Set the initial time.
    	Instant now = Instant.now();
    	setCreated(now);
   	    
   }
    
    /* **************************************************************************** */
    /*                                Public Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* toString:                                                                    */
    /* ---------------------------------------------------------------------------- */
    @Override
    public String toString() {return TapisUtils.toString(this);}

   
    /* **************************************************************************** */
    /*                                  Accessors                                   */
    /* **************************************************************************** */
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getTenant() {
		return tenant;
	}

	public void setTenant(String tenant) {
		this.tenant = tenant;
	}
    
	public String getCreatedby() {
		return createdby;
	}

	public void setCreatedby(String createdby) {
		this.createdby = createdby;
	}
		
	@Schema(type = "string")
	public Instant getCreated() {
		return created;
	}

	public void setCreated(Instant created) {
		this.created = created;
	}

	
	public String getJobUuid() {
		return jobUuid;
	}

	public void setJobUuid(String jobUuid) {
		this.jobUuid = jobUuid;
	}

	
	

	public String getGrantee() {
		return grantee;
	}

	public String getGrantor() {
		return grantor;
	}

	public void setGrantor(String grantor) {
		this.grantor = grantor;
	}

	public void setUserSharedWith(String grantee) {
		this.grantee = grantee;
	}

	public JobResourceShare getJobResource() {
		return jobResource;
	}

	public void setJobResource(JobResourceShare jobResource) {
		this.jobResource = jobResource;
	}

	public JobTapisPermission getJobPermission() {
		return jobPermission;
	}

	public void setJobPermission(JobTapisPermission jobPermission) {
		this.jobPermission = jobPermission;
	}

	
 
}
