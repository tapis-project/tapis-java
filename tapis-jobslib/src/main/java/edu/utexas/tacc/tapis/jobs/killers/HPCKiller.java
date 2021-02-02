package edu.utexas.tacc.tapis.jobs.killers;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;

public class HPCKiller extends AbstractJobKiller {

	public HPCKiller(Job job, TSystem executionSystem)
	{
	    super(job, executionSystem);
	}

    /* (non-Javadoc)
     * @see org.iplantc.service.jobs.managers.killers.AbstractJobKiller#getCommand()
     */
    @Override
    protected String getCommand() 
    {
        return null; // TODO:
//        String remoteJobId = _job.getRemoteJobId();
//        if (StringUtils.isBlank(remoteJobId)) return null;
//        return _execSystem.getSchedulerType().getBatchKillCommand() + " " + remoteJobId;
    }
    
    /* (non-Javadoc)
     * @see org.iplantc.service.jobs.managers.killers.AbstractJobKiller#getAltCommand()
     */
    @Override
    protected String getAltCommand() 
    {
        return null;
//        // Protect ourselves from going down a rabbit hole.
//        if (StringUtils.isBlank(_job.getRemoteJobId())) return null;
//        
//    	// if the response was empty, the job could be done, but the scheduler could only 
//		// recognize numeric job ids. Let's try again with just the numeric part
//    	String numericJobId = _job.getNumericRemoteJobId();
//    	
//    	// No point in trying again if nothing new can happen.
//		if (StringUtils.isEmpty(numericJobId) || 
//			StringUtils.equals(numericJobId, _job.getRemoteJobId())) {
//			return null;
//		}
//		else {
//			return _execSystem.getSchedulerType().getBatchKillCommand() + " " + numericJobId;
//		}
    }
}
