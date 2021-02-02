package edu.utexas.tacc.tapis.jobs.killers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;

/** Kill a job on the remote system.
 * @author rcardone
 */
public abstract class AbstractJobKiller 
 implements JobKiller 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
	private static final Logger _log = LoggerFactory.getLogger(AbstractJobKiller.class);
	
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
	// Constructor values.
    protected final Job             _job;
    protected final TSystem         _execSystem;

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    /** The instance must of non-null parameters or else a runtime exception is
     * thrown.
     * 
     * @param job the job whose remote execution is to be terminated
     * @param the execution on which the job is remotely running
     */
	protected AbstractJobKiller(Job job, TSystem execSystem)
	{
        // Make sure both the job and its context are not null.
        if (job == null) {
            String msg = MsgUtils.getMsg("ALOE_NULL_PARAMETER", "AbstractJobKiller", "job");
            _log.error(msg);
            throw new TapisRuntimeException(msg);
        }
        if (execSystem == null) {
            String msg = MsgUtils.getMsg("ALOE_NULL_PARAMETER", "AbstractJobKiller", "execSystem");
            _log.error(msg);
            throw new TapisRuntimeException(msg);
        }
        
        // We're good.
	    _job = job;
	    _execSystem = execSystem;
	}

    /* ********************************************************************** */
    /*                            Abstract Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getCommand:                                                            */
    /* ---------------------------------------------------------------------- */
    /** Provides the actual command that should be invoked on the remote
     * system to kill the job.
     * 
     * @return 
     */
    protected abstract String getCommand();
    
    /* ---------------------------------------------------------------------- */
    /* getAltCommand:                                                         */
    /* ---------------------------------------------------------------------- */
    /** Provides the actual command that should be invoked on the remote
     * system to kill the job using the shortened numeric remote local job id.
     * 
     * @return the same as {@link #getCommand()}, but with the {@link Job#getNumericLocalJobId()}, 
     *         null if they are the same
     */
    protected abstract String getAltCommand();

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* attack:                                                                */
    /* ---------------------------------------------------------------------- */
	@Override
	public void attack()
	{
        // Get the command to kill the remote job process.  There's 
        // nothing to do if the remote job id isn't available.
        String remoteJobKillCommand = getCommand();
        if (remoteJobKillCommand == null) return;
        
//        // Best effort kill.
//		RemoteSubmissionClient remoteSubmissionClient = null;
//		try
//		{
//		    // Initialize the connection to the remote system.
//			remoteSubmissionClient = _execSystem.getRemoteSubmissionClient();
//			
//			// Resolve the startupScript and generate the command to run it and 
//			// log the response to the "/.agave.log" file.
//			String startupScriptCommand = getStartupScriptCommand();
//			
//			// Initialize variables.
//			String result = null;
//			String cmd = startupScriptCommand + " ; " + remoteJobKillCommand;
//			if (_log.isDebugEnabled())
//			    _log.debug(MsgUtils.getMsg("JOBS_KILL_CMD", _job.getUuid(), _job.getExecSystemId(), 
//			                              _job.getRemoteJobId(), cmd));
//			
//			// Run the aggregate command on the remote system.
//			try {result = remoteSubmissionClient.runCommand(cmd);}
//			    catch (Exception e) {
//			        _log.warn(MsgUtils.getMsg("JOBS_KILL_CMD_FAILED", _job.getUuid(), _job.getExecSystemId(), 
//			                                  _job.getRemoteJobId(), cmd));
//			    }
//			reportResult(result, cmd);
//			
//			// If the response was empty, the job could be done, but the scheduler might only 
//			// recognize numeric job ids. Let's try again with just the numeric part
//			if (StringUtils.isBlank(result)) 
//			{
//			    // Is there an alternate command to try?
//				String altCommand = getAltCommand();
//				if (!StringUtils.isBlank(altCommand)) 
//				{
//				    // Create the new command string.
//				    cmd = startupScriptCommand + " ; " + altCommand;
//		            if (_log.isDebugEnabled())
//		                _log.debug(MsgUtils.getMsg("JOBS_KILL_CMD", _job.getUuid(), _job.getExecSystemId(), 
//		                                          _job.getRemoteJobId(), cmd));
//		            
//		            // Last chance.
//		            try {result = remoteSubmissionClient.runCommand(cmd);}
//	                    catch (Exception e) {
//	                        _log.warn(MsgUtils.getMsg("JOBS_KILL_CMD_FAILED", _job.getUuid(), _job.getExecSystemId(), 
//	                                                  _job.getRemoteJobId(), cmd));
//	                    }
//		            reportResult(result, cmd);
//				}
//			}
//		}
//		catch (Exception e) {
//		    // There's no point in passing execution errors back to the caller,
//		    // so we just log the problem and move on.
//		    _log.error(MsgUtils.getMsg("JOBS_KILL_ERROR", _job.getUuid(), _job.getExecSystemId(), 
//                       _job.getRemoteJobId(), e.getMessage()), e);
//		}
//		finally {
//		    // Always shutdown the connection.
//		    if (remoteSubmissionClient != null) 
//		        try {remoteSubmissionClient.close();} catch (Throwable e) {}
//		}
	}
	
    /* ********************************************************************** */
    /*                             Private Methods                            */
    /* ********************************************************************** */
//    /* ---------------------------------------------------------------------- */
//    /* resolveStartupScriptMacros:                                            */
//    /* ---------------------------------------------------------------------- */
//	/** Resolve macros in the startup script.
//     * @param startupScript
//     * @return the resolved string
//     */
//    private String resolveStartupScriptMacros(String startupScript) 
//	throws TapisException 
//	{
//		if (StringUtils.isBlank(startupScript)) {
//			return null;
//		}
//		else {
//			String resolvedStartupScript = startupScript;
//			for (StartupScriptSystemVariableType macro: StartupScriptSystemVariableType.values()) {
//				resolvedStartupScript = StringUtils.replace(resolvedStartupScript, "${" + macro.name() + "}", macro.resolveForSystem(_execSystem));
//			}
//			
//			for (StartupScriptJobVariableType macro: StartupScriptJobVariableType.values()) {
//				resolvedStartupScript = StringUtils.replace(resolvedStartupScript, "${" + macro.name() + "}", macro.resolveForJob(_job));
//			}
//			
//			return resolvedStartupScript;
//		}
//	}
//	
//    /* ---------------------------------------------------------------------- */
//    /* getStartupScriptCommand:                                               */
//    /* ---------------------------------------------------------------------- */
//	/** Return the startup script or a printable message.
//	 * @return startup script or no-script message 
//	 */
//    private String getStartupScriptCommand() throws TapisException 
//	{
//		String startupScriptCommand = "";
//		if (!StringUtils.isBlank(_execSystem.getStartupScript())) {
//			String resolvedstartupScript = resolveStartupScriptMacros(_execSystem.getStartupScript());
//			
//			if (resolvedstartupScript != null) 
//				startupScriptCommand = String.format("echo $(source %s 2>&1) >> /dev/null ",
//						                             resolvedstartupScript);
//		}
//		
//		if (StringUtils.isBlank(startupScriptCommand)) {
//			startupScriptCommand = String.format("echo 'No startup script defined. Skipping...' >> /dev/null ");
//		}
//		
//		return startupScriptCommand;
//	}
    
    /* ---------------------------------------------------------------------- */
    /* reportResult:                                                          */
    /* ---------------------------------------------------------------------- */
    /** Log the result of an issued kill command.
     * 
     * @param result the result string returned from the remote system
     * @param cmd the command that was issued
     */
    private void reportResult(String result, String cmd)
     throws TapisException
    {
//        // There's no point in continuing if the required logging level isn't active.
//        if (!_log.isWarnEnabled()) return;
//        
//        // Inspect results.
//        if (StringUtils.isEmpty(result)) 
//        {
//            if (_execSystem.getSchedulerType() != SchedulerType.FORK) 
//                _log.warn(MsgUtils.getMsg("JOBS_KILL_CMD_NULL_RESULT", _job.getUuid(), _job.getSystemId(), 
//                                          _job.getRemoteJobId(), cmd));
//              else 
//                _log.warn(MsgUtils.getMsg("JOBS_KILL_NOT_RUNNING", _job.getUuid(), _job.getSystemId(), 
//                                          _job.getRemoteJobId()));
//        }
//        else 
//        {
//            // Look for distinguished phrases in the result string.
//            String[] notFoundTerms = new String[] {"does not exist", "has deleted job", "couldn't find"};
//            for (String notfoundTerm: notFoundTerms) 
//            {
//                if (result.toLowerCase().contains(notfoundTerm)) {
//                     _log.warn(MsgUtils.getMsg("JOBS_KILL_NOT_FOUND", _job.getUuid(), _job.getSystemId(), 
//                                               _job.getRemoteJobId(), result));
//                     break;
//                }
//            }
//        }
    }
}
