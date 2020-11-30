package edu.utexas.tacc.tapis.jobs.dao;

import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

@Test(groups={"integration"})
public class CreateJobWithConstraintsTest 
{
	// Number of jobs created.
	private static final int ITERATIONS = 4000;
	
	@Test
	public void jobTest() throws TapisException
	{
		// Access the database.
		var dao = new JobsDao();
		
		// Get all jobs.
		var jobList = dao.getJobs();
		System.out.println("Number of existing jobs records: " + jobList.size());		
				
		// Insert job record.
		for (int i = 0; i < ITERATIONS; i++) {
			Job job = initJob(i);
			dao.createJob(job);
		}
		
		// Get all job records.
		jobList = dao.getJobs();
		System.out.println("Number of existing jobs records: " + jobList.size());	

		// Retrieve the newly created job record.
		if (!jobList.isEmpty()) {
			var retrievedJob = jobList.get(jobList.size()-1);
			System.out.println("Parameters for job " + retrievedJob.getId() + ": " 
			                   + retrievedJob.getParameters());
		}
	}
	
	/* ********************************************************************** */
	/*                            Private Methods                             */
	/* ********************************************************************** */
	private Job initJob(int iteration)
	{
		var job = new Job();
		
		// Required fields
		job.setName("test1job");
		job.setOwner("bud");
		job.setTenant("fakeTenant");
		job.setDescription("This is a fake job that will never run");

	    job.setAppId("fakeAppId");
	    job.setAppVersion("1.0");
	    
	    job.setExecSystemId("fakeExecSystemId");
	    
	    job.setTapisQueue("fakeTapisQueue");
	    job.setCreatedby("mary");
	    job.setCreatedbyTenant("maryTenant");
	    
	    // Assign key names.
	    String key1 = "key" + (iteration * 5 + 1);
	    String key2 = "key" + (iteration * 5 + 2);
	    String key3 = "key" + (iteration * 5 + 3);
	    String key4 = "key" + (iteration * 5 + 4);
	    String key5 = "key" + (iteration * 5 + 5);
		
	    // Optional fields.
	    String constraints = "{\"execSystemConstraints\": [\""
	    		                + key1 + " > 40 AND " 
	    		                + key2 + " != 'active' AND "
	    		                + key3 + " in (4, 6, 8) AND "
	    		                + "(" + key4 + " >= 42 OR "
	    		                + key5 + " like 'abc%') "
				   				+ "\"]}";
	    job.setExecSystemConstraints(constraints);
	    
	    String parms = "{\"parameters\": {\"appArgs\": [{\"arg\": \"-f x.txt\"}, {\"arg\": \"banana\"}]}}";
	    job.setParameters(parms);
	    
		return job;
	}
}
