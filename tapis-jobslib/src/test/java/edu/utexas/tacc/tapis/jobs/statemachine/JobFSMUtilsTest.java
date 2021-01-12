package edu.utexas.tacc.tapis.jobs.statemachine;

import org.testng.Assert;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;

/** This test program issues a small number of legal and illegal state transitions
 * validation calls.  The goal is to test the utility code and leave the 
 * comprehensive state machine validation testing to JobFSMTest. 
 * 
 * @author rcardone
 *
 */
@Test(groups={"unit"})
public class JobFSMUtilsTest
{
    /* ********************************************************************** */
    /*                              Test Methods                              */
    /* ********************************************************************** */    
    /* ---------------------------------------------------------------------- */
    /* sampleTest:                                                            */
    /* ---------------------------------------------------------------------- */
    @Test(enabled=true)
    public void sampleTest()
    {   
        // Result of transition test.
        boolean result;
        
        // ----- Legal transitions.
        result = JobFSMUtils.hasTransition(JobStatusType.PENDING, JobStatusType.PENDING);
        Assert.assertTrue(result, "Failed on a legal transaction!");

        result = JobFSMUtils.hasTransition(JobStatusType.QUEUED, JobStatusType.RUNNING);
        Assert.assertTrue(result, "Failed on a legal transaction!");

        result = JobFSMUtils.hasTransition(JobStatusType.RUNNING, JobStatusType.ARCHIVING);
        Assert.assertTrue(result, "Failed on a legal transaction!");

        result = JobFSMUtils.hasTransition(JobStatusType.ARCHIVING, JobStatusType.FINISHED);
        Assert.assertTrue(result, "Failed on a legal transaction!");

        // ----- Illegal transitions.
        result = JobFSMUtils.hasTransition(JobStatusType.FINISHED, JobStatusType.RUNNING);
        Assert.assertFalse(result, "Failed to identify an illegal transaction!");

        result = JobFSMUtils.hasTransition(JobStatusType.PAUSED, null);
        Assert.assertFalse(result, "Failed to identify an illegal transaction!");
        
        result = JobFSMUtils.hasTransition(null, JobStatusType.PENDING);
        Assert.assertFalse(result, "Failed to identify an illegal transaction!");
        
        result = JobFSMUtils.hasTransition(JobStatusType.FINISHED, JobStatusType.STAGING_INPUTS);
        Assert.assertFalse(result, "Failed to identify an illegal transaction!");
        
        result = JobFSMUtils.hasTransition(JobStatusType.FAILED, JobStatusType.FINISHED);
        Assert.assertFalse(result, "Failed to identify an illegal transaction!");
   }
}
