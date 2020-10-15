package edu.utexas.tacc.tapis.jobs.dao;

import org.testng.annotations.Test;

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
		
		// Get all job records.
		jobList = dao.getJobs();
		System.out.println("Number of existing jobs records: " + jobList.size());	

		// Retrieve the newly created job record.

	}
}
