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
		if (!jobList.isEmpty()) {
			var retrievedJob = jobList.get(jobList.size()-1);
			System.out.println("Parameters for job " + retrievedJob.getId() + ": " 
			                   + retrievedJob.getParameters());
		}
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
	    String json = 
	    		"{\"parameters\": {\"appArgs\": [{\"arg\": \"x\"}, {\"arg\": \"-f y.txt\"}], "
				+ "                \"containerArgs\": [{\"arg\": \"-v 3\", "
				+ "                                     \"meta\": {\"name\": \"bud\", \"required\": true, "
				+ "                                        \"kv\": [{\"key\": \"k1\", \"value\": \"v1\"}, "
				+ "                                                 {\"key\": \"k2\", \"value\": \"v2\"}]}}],"
				+ "                \"schedulerOptions\": [{\"arg\": \"-A 34493\"}], "
				+ "                \"envVariables\": [{\"key\": \"TAPIS_SERVICE\", \"value\": \"jobs\"}]"
				+ "}}";
	    job.setParameters(json);
	    
		return job;
	}
}
