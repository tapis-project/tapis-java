package edu.utexas.tacc.tapis.jobs.killers;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;

public class ProcessKiller extends AbstractJobKiller 
{
	public ProcessKiller(Job job, TSystem execSystem)
	{
		super(job, execSystem);
	}

    @Override
    protected String getCommand() 
    {
        String remoteJobId = _job.getRemoteJobId();
        if (StringUtils.isBlank(remoteJobId)) return null;
        return "kill -9 " + remoteJobId;
    }

	/** 
	 * Will always return null as process id are numeric
	 * @see org.iplantc.service.jobs.managers.killers.AbstractJobKiller#getAltCommand()
	 */
	@Override
	protected String getAltCommand() 
	{
		return null;
	}
}
