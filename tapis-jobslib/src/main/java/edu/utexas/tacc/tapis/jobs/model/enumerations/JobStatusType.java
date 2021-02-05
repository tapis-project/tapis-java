package edu.utexas.tacc.tapis.jobs.model.enumerations;

public enum JobStatusType
{
    PENDING("Job processing beginning"),
  
    PROCESSING_INPUTS("Identifying input files for staging"),
    STAGING_INPUTS("Transferring job input data to execution system"),
    
    STAGING_JOB("Staging runtime assets to execution system"),
    SUBMITTING_JOB("Submitting job to execution system"),
    
    QUEUED("Job queued to execution system queue"), 
    RUNNING("Job running on execution system"), 

    ARCHIVING("Transferring job output to archive system"), 
  
    BLOCKED("Job blocked"),
    PAUSED("Job processing suspended"),
    
    FINISHED("Job completed successfully"), 
    CANCELLED("Job execution intentionally stopped"), 
    FAILED("Job failed"); 
  
    // ---- Fields
	private final String _description;
	
	// ---- Constructor
	JobStatusType(String description){_description = description;}
	
	// ---- Instance Methods
	public String  getDescription(){return _description;}
	public boolean isActive(){return isActive(this);}
	public boolean isTerminal(){return isTerminal(this);}
	public boolean isExecuting(){return isExecuting(this);}

    @Override
    public String toString(){return name();}
    
	// ---- Static Methods
	public static boolean isActive(JobStatusType status)
	{
		return !isTerminal(status) && (status != PAUSED) && (status != BLOCKED);
	}

	public static boolean isTerminal(JobStatusType status)
	{
		return status == FINISHED || status == FAILED || status == CANCELLED;
	}
	
	public static boolean isExecuting(JobStatusType status)
	{
		return status == RUNNING || status == QUEUED;
	}
	
	// Construct the string on non-active to be used 
	// SQL IN clauses.
	public static String getNonActiveSQLString()
	{
	    // Construct the quoted string with commas.
	    return "'" + FINISHED.name()  + "::job_status_enum', '"
	               + FAILED.name()    + "::job_status_enum', '"
	               + CANCELLED.name() + "::job_status_enum', '"
	               + BLOCKED.name()   + "::job_status_enum', '" 
	               + PAUSED.name()    + "::job_status_enum'";
	}

	// Construct the string on terminal to be used
	// SQL IN clauses.
	public static String getTerminalSQLString()
	{
		// Construct the quoted string with commas.
		return "'"  + FINISHED.name()  + "::job_status_enum', '"
					+ FAILED.name()    + "::job_status_enum', '"
					+ CANCELLED.name() + "::job_status_enum'";
	}

}