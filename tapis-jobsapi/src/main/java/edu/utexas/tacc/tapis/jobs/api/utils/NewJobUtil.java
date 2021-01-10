package edu.utexas.tacc.tapis.jobs.api.utils;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.config.RuntimeParameters;
import edu.utexas.tacc.tapis.jobs.dao.JobsDao;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.providers.email.EmailClient;
import edu.utexas.tacc.tapis.shared.providers.email.EmailClientFactory;
import edu.utexas.tacc.tapis.shared.utils.HTMLizer;

public class NewJobUtil 
{
	/* **************************************************************************** */
	/*                                   Constants                                  */
	/* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(NewJobUtil.class);

    // Cache of users not allowed to submit jobs. Key is <tenant_id>_<username>
    // NOTE: Table must be modified manually. When it is modified the service must be re-started.
    private static Set userDenyList= null;

    /* **************************************************************************** */
    /*                                Public Methods                                */
    /* **************************************************************************** */
	/* ---------------------------------------------------------------------------- */
    /* failJob:                                                                     */
	/* ---------------------------------------------------------------------------- */
	/** Mark the job as failed in the database.
	 * 
	 * @param jobsDao the db access object
	 * @param job the failed job
	 * @param failMsg the failure message
	*/
	public static void failJob(JobsDao jobsDao, Job job, String failMsg)
	{
		// Fail the job.  Note that current status used in the transition 
	    // to FAILED is the status of the job as defined in the db.
//		try {jobsDao.failJob("submitJob", job, failMsg);}
//		  	catch (Exception e) {
//	            // Swallow exception.
//	            String msg = MsgUtils.getMsg("JOBS_ZOMBIE_ERROR", 
//	                                         job.getUuid(), job.getTenant(), "submitJob");
//	            _log.error(msg, e);
//	            
//	            // Try to send the zombie email.
//	            sendZombieEmail(job, msg);
//		  	}
    }
	  
    /* **************************************************************************** */
    /*                               Private Methods                                */
    /* **************************************************************************** */
	/* ---------------------------------------------------------------------------- */
	/* sendZombieEmail:                                                             */
	/* ---------------------------------------------------------------------------- */
	/** Send an email to alert support that a zombie job exists.
	 * 
	 * @param job the job whose status update failed
	 * @param zombiMsg failure message
	 */
	private static void sendZombieEmail(Job job, String zombiMsg)
	{
		String subject = "Zombie Job Alert: " + job.getUuid() + " is in a zombie state.";
	    try {
	    	  RuntimeParameters runtime = RuntimeParameters.getInstance();
	    	  EmailClient client = EmailClientFactory.getClient(runtime);
	    	  client.send(runtime.getSupportName(),
	    			  runtime.getSupportEmail(),
	    			  subject,
	    			  zombiMsg, HTMLizer.htmlize(zombiMsg));
	    }
	    catch (TapisException e1) {
	    	  // log msg that we tried to send email notice to support.
	    	  RuntimeParameters runtime = RuntimeParameters.getInstance();
	    	  String recipient = runtime == null ? "unknown" : runtime.getSupportEmail();
	    	  String msg = MsgUtils.getMsg("ALOE_SUPPORT_EMAIL_ERROR", recipient, subject, e1.getMessage());
	    	  _log.error(msg, e1);
	    }
	}
}
