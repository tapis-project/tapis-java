package edu.utexas.tacc.tapis.jobs.dao;

import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

@Test(groups={"integration"})
public class CreateJobTest 
{
	@Test
	public void jobTest() throws TapisException
	{
		// Access the database.
		var dao = new JobsDao();
		
		// Get all jobs.
		var jobList = dao.getJobs();
		System.out.println("Number of existing jobs records: " + jobList.size());		
				
		// Insert job record.
		Job job = initJob();
		dao.createJob(job);
		
		// Get all job records.
		jobList = dao.getJobs();
		System.out.println("Number of existing jobs records: " + jobList.size());	

		// Retrieve the newly created job record.

	}
	
	/* ********************************************************************** */
	/*                            Private Methods                             */
	/* ********************************************************************** */
	private Job initJob()
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
		
	    // Optional fields.
	    String json = "{\"key1\": \"value1\", \"key2\": 5}";
	    job.setExecSystemConstraints(json);
	    
		return job;
	}
}
